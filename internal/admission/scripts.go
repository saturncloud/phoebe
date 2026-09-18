package admission

import "github.com/redis/go-redis/v9"

// The counter store is intentionally compact: every operation is one Lua
// transaction over four keys in one Redis Cluster slot. Expired lease and
// fixed-window reaping occurs before every mutation, so replica death cannot
// leak reservations past leaseTtl and retired dynamic scopes cannot grow the
// counters hash forever.
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
  local base=field(s,dim)
  if get(base..'_bucket') ~= b then
    redis.call('HSET',KEYS[1],base..'_bucket',b,base..'_count',0)
  end
  add(base..'_count',delta)
  -- The member includes the bucket so an old expiry can never delete a newer
  -- bucket for the same scope/dimension. A bounded reaper amortizes churn.
  local member=cjson.encode({field=base,bucket=b})
  redis.call('ZADD',KEYS[4],(b+1)*s.window_ms,member)
end
local function window_retry_ms(s, now)
  local remaining=s.window_ms - (now % s.window_ms)
  if remaining <= 0 then return s.window_ms end
  return remaining
end
local function release_record(rec)
  for _,s in ipairs(rec.scopes) do
    add(field(s,'active'), -1)
    add(field(s,'reserved_decode_slots'), -1)
    add(field(s,'output'), -rec.output)
    add(field(s,'genreserved'), -rec.output)
    add(field(s,'total_prompt_reserved'), -rec.estimated_input)
    add(field(s,'uncached_prompt_reserved'), -rec.estimated_input)
    if rec.adapter == 1 then add(field(s,'adapters'), -1) end
    if rec.prefill then add(field(s,'prefills'), -1); add(field(s,'prompt'), -rec.prompt) end
    if rec.cold then add(field(s,'cold'), -1) end
  end
end
local function reap(now)
  -- Keep each atomic mutation bounded. Leases left behind still hold their
  -- counters, so a backlog can only reject conservatively until later calls
  -- drain it; it can never create unaccounted capacity.
  local expired=redis.call('ZRANGEBYSCORE',KEYS[3],'-inf',now,'LIMIT',0,100)
  for _,id in ipairs(expired) do
    local raw=redis.call('HGET',KEYS[2],id)
    if raw then release_record(cjson.decode(raw)); redis.call('HDEL',KEYS[2],id) end
    redis.call('ZREM',KEYS[3],id)
  end
  local windows=redis.call('ZRANGEBYSCORE',KEYS[4],'-inf',now,'LIMIT',0,100)
  for _,member in ipairs(windows) do
    local item=cjson.decode(member)
    if get(item.field..'_bucket') == tonumber(item.bucket) then
      redis.call('HDEL',KEYS[1],item.field..'_bucket',item.field..'_count')
    end
    redis.call('ZREM',KEYS[4],member)
  end
end
`

var admitScript = redis.NewScript(luaHelpers + `
local q=cjson.decode(ARGV[1]); q.now=current_time_ms(); reap(q.now)
-- A caller may lose the reply after this script commits. Retrying the exact
-- request id must recover the existing lease rather than reserve every
-- dimension a second time.
if redis.call('HEXISTS',KEYS[2],q.id) == 1 then return {1} end
for _,s in ipairs(q.scopes) do
  local contract=s.contractual and 1 or 0
  local checks={{'active',s.active,1},{'prefills',s.prefills,1},{'reserved_decode_slots',s.reserved_decode_slots,1},{'prompt',s.prompt,q.prompt},{'output',s.output,q.output},{'adapters',s.adapters,q.adapter}}
  for _,c in ipairs(checks) do if c[2] > 0 and get(field(s,c[1])) + c[3] > c[2] then return {0,s.name,c[1],contract} end end
  if s.requests > 0 and window_get(s,'requests',q.now)+1 > s.requests then return {0,s.name,'requests',contract,window_retry_ms(s,q.now)} end
  if s.total_prompt > 0 and window_get(s,'total_prompt',q.now)+get(field(s,'total_prompt_reserved'))+q.estimated_input > s.total_prompt then return {0,s.name,'total_prompt_tokens',contract,window_retry_ms(s,q.now)} end
  if s.uncached_prompt > 0 and window_get(s,'uncached_prompt',q.now)+get(field(s,'uncached_prompt_reserved'))+q.estimated_input > s.uncached_prompt then return {0,s.name,'uncached_prompt_tokens',contract,window_retry_ms(s,q.now)} end
  if s.generated > 0 and window_get(s,'generated',q.now)+get(field(s,'genreserved'))+q.output > s.generated then return {0,s.name,'generated_tokens',contract,window_retry_ms(s,q.now)} end
end
local rec={scopes=q.scopes,prompt=q.prompt,estimated_input=q.estimated_input,output=q.output,adapter=q.adapter,prefill=true,cold=false,admitted_at=q.now}
for _,s in ipairs(q.scopes) do
  add(field(s,'active'),1); add(field(s,'prefills'),1); add(field(s,'reserved_decode_slots'),1); add(field(s,'prompt'),q.prompt); add(field(s,'output'),q.output); add(field(s,'genreserved'),q.output)
  add(field(s,'total_prompt_reserved'),q.estimated_input); add(field(s,'uncached_prompt_reserved'),q.estimated_input)
  if q.adapter == 1 then add(field(s,'adapters'),1) end
  if s.requests > 0 then window_add(s,'requests',q.now,1) end
end
redis.call('HSET',KEYS[2],q.id,cjson.encode(rec)); redis.call('ZADD',KEYS[3],q.now+q.lease_ms,q.id)
return {1}
`)

// abandonScript is the compensating transaction for an Admit whose commit
// remains indeterminate after an idempotent retry. It is safe whether the
// original transaction committed or not, and prevents a lost reply from
// holding capacity for the full lease TTL.
var abandonScript = redis.NewScript(luaHelpers + `
local now=current_time_ms(); reap(now); local raw=redis.call('HGET',KEYS[2],ARGV[1])
if not raw then return {1} end
local rec=cjson.decode(raw); release_record(rec)
-- The request never reached the engine, so undo its RPM charge when cleanup
-- occurs in the same fixed window. Never subtract from a newer bucket.
for _,s in ipairs(rec.scopes) do
  if s.requests > 0 and rec.admitted_at and bucket(s,rec.admitted_at) == bucket(s,now) then
    window_add(s,'requests',now,-1)
  end
end
redis.call('HDEL',KEYS[2],ARGV[1]); redis.call('ZREM',KEYS[3],ARGV[1]); return {1}
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
    local contract=s.contractual and 1 or 0
    if s.cold > 0 and get(field(s,'cold'))+1 > s.cold then return {0,s.name,'cold_holds',contract} end
    if s.wakes > 0 and window_get(s,'wakes',now)+1 > s.wakes then return {0,s.name,'wakes',contract,window_retry_ms(s,now)} end
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
local total_prompt=tonumber(ARGV[4]) or 0; local uncached_prompt=tonumber(ARGV[5]) or 0
release_record(rec)
for _,s in ipairs(rec.scopes) do
  if total_prompt > 0 and s.total_prompt > 0 then window_add(s,'total_prompt',now,total_prompt) end
  if uncached_prompt > 0 and s.uncached_prompt > 0 then window_add(s,'uncached_prompt',now,uncached_prompt) end
  if generated > 0 and s.generated > 0 then window_add(s,'generated',now,generated) end
end
redis.call('HDEL',KEYS[2],ARGV[1]); redis.call('ZREM',KEYS[3],ARGV[1]); return {1}
`)

var renewScript = redis.NewScript(luaHelpers + `
local now=current_time_ms(); reap(now)
if redis.call('HEXISTS',KEYS[2],ARGV[1]) == 0 then return 0 end
redis.call('ZADD',KEYS[3],now+tonumber(ARGV[2]),ARGV[1]); return 1
`)
