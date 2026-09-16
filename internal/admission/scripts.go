package admission

import "github.com/redis/go-redis/v9"

// The counter store is intentionally compact: every operation is one Lua
// transaction over three keys in one Redis Cluster slot. Expired lease reaping
// occurs before every mutation, so replica death cannot leak reservations past
// leaseTtl.
const luaHelpers = `
local function get(field) return tonumber(redis.call('HGET', KEYS[1], field) or '0') end
local function current_time_ms()
  local t=redis.call('TIME'); return tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)
end
local function add(field, delta)
  local v = redis.call('HINCRBY', KEYS[1], field, delta)
  if tonumber(v) <= 0 then redis.call('HDEL', KEYS[1], field) end
end
local function bucket(s, now) return math.floor(now / s.window_ms) end
local function field(s, dim) return s.id .. '|' .. dim end
local function window_get(s, dim, now)
  local b=bucket(s,now)
  if get(field(s,dim..'_bucket')) ~= b then return 0 end
  return get(field(s,dim..'_count'))
end
local function window_add(s, dim, now, delta)
  local b=bucket(s,now)
  if get(field(s,dim..'_bucket')) ~= b then
    redis.call('HSET',KEYS[1],field(s,dim..'_bucket'),b,field(s,dim..'_count'),0)
  end
  add(field(s,dim..'_count'),delta)
end
local function release_record(rec)
  for _,s in ipairs(rec.scopes) do
    add(field(s,'active'), -1)
    add(field(s,'decodes'), -1)
    add(field(s,'output'), -rec.output)
    add(field(s,'genreserved'), -rec.output)
    if rec.adapter == 1 then add(field(s,'adapters'), -1) end
    if rec.prefill then add(field(s,'prefills'), -1); add(field(s,'prompt'), -rec.prompt) end
    if rec.cold then add(field(s,'cold'), -1) end
  end
end
local function reap(now)
  local expired=redis.call('ZRANGEBYSCORE',KEYS[3],'-inf',now)
  for _,id in ipairs(expired) do
    local raw=redis.call('HGET',KEYS[2],id)
    if raw then release_record(cjson.decode(raw)); redis.call('HDEL',KEYS[2],id) end
    redis.call('ZREM',KEYS[3],id)
  end
end
`

var admitScript = redis.NewScript(luaHelpers + `
local q=cjson.decode(ARGV[1]); q.now=current_time_ms(); reap(q.now)
for _,s in ipairs(q.scopes) do
  local checks={{'active',s.active,1},{'prefills',s.prefills,1},{'decodes',s.decodes,1},{'prompt',s.prompt,q.prompt},{'output',s.output,q.output},{'adapters',s.adapters,q.adapter}}
  for _,c in ipairs(checks) do if c[2] > 0 and get(field(s,c[1])) + c[3] > c[2] then return {0,s.name,c[1]} end end
  if s.requests > 0 and window_get(s,'requests',q.now)+1 > s.requests then return {0,s.name,'requests'} end
  if s.generated > 0 and window_get(s,'generated',q.now)+get(field(s,'genreserved'))+q.output > s.generated then return {0,s.name,'generated_tokens'} end
end
local rec={scopes=q.scopes,prompt=q.prompt,output=q.output,adapter=q.adapter,prefill=true,cold=false}
for _,s in ipairs(q.scopes) do
  add(field(s,'active'),1); add(field(s,'prefills'),1); add(field(s,'decodes'),1); add(field(s,'prompt'),q.prompt); add(field(s,'output'),q.output); add(field(s,'genreserved'),q.output)
  if q.adapter == 1 then add(field(s,'adapters'),1) end
  if s.requests > 0 then window_add(s,'requests',q.now,1) end
end
redis.call('HSET',KEYS[2],q.id,cjson.encode(rec)); redis.call('ZADD',KEYS[3],q.now+q.lease_ms,q.id)
return {1}
`)

var transitionScript = redis.NewScript(luaHelpers + `
reap(current_time_ms()); local raw=redis.call('HGET',KEYS[2],ARGV[1]); if not raw then return {1} end
local rec=cjson.decode(raw)
if ARGV[2] == 'prefill' and rec.prefill then
  for _,s in ipairs(rec.scopes) do add(field(s,'prefills'),-1); add(field(s,'prompt'),-rec.prompt) end
  rec.prefill=false; redis.call('HSET',KEYS[2],ARGV[1],cjson.encode(rec))
end
return {1}
`)

var coldScript = redis.NewScript(luaHelpers + `
local now=current_time_ms(); reap(now); local raw=redis.call('HGET',KEYS[2],ARGV[1]); if not raw then return {1} end
local rec=cjson.decode(raw)
if ARGV[2] == 'begin' and not rec.cold then
  for _,s in ipairs(rec.scopes) do
    if s.cold > 0 and get(field(s,'cold'))+1 > s.cold then return {0,s.name,'cold_holds'} end
    if s.wakes > 0 and window_get(s,'wakes',now)+1 > s.wakes then return {0,s.name,'wakes'} end
  end
  for _,s in ipairs(rec.scopes) do add(field(s,'cold'),1); if s.wakes > 0 then window_add(s,'wakes',now,1) end end
  rec.cold=true; redis.call('HSET',KEYS[2],ARGV[1],cjson.encode(rec))
elseif ARGV[2] == 'end' and rec.cold then
  for _,s in ipairs(rec.scopes) do add(field(s,'cold'),-1) end
  rec.cold=false; redis.call('HSET',KEYS[2],ARGV[1],cjson.encode(rec))
end
return {1}
`)

var finishScript = redis.NewScript(luaHelpers + `
local now=current_time_ms(); reap(now); local raw=redis.call('HGET',KEYS[2],ARGV[1]); if not raw then return {1} end
local rec=cjson.decode(raw); local generated=tonumber(ARGV[3]) or 0
release_record(rec)
for _,s in ipairs(rec.scopes) do if generated > 0 and s.generated > 0 then window_add(s,'generated',now,generated) end end
redis.call('HDEL',KEYS[2],ARGV[1]); redis.call('ZREM',KEYS[3],ARGV[1]); return {1}
`)

var renewScript = redis.NewScript(luaHelpers + `
local now=current_time_ms(); reap(now)
if redis.call('HEXISTS',KEYS[2],ARGV[1]) == 0 then return 0 end
redis.call('ZADD',KEYS[3],now+tonumber(ARGV[2]),ARGV[1]); return 1
`)
