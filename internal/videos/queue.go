package videos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue is one Valkey stream per endpoint, holding the ids of its queued jobs
// in arrival order (first in, first out; design 5.4). The stream orders and
// wakes; it is not the record. A job's status lives in Postgres, every claim
// is checked there, and a stream that lost its entries (a Valkey restart
// without persistence) is rebuilt from the queued rows by the reaper.
//
// Entries are acknowledged and deleted once a dispatcher has claimed the job
// or found it no longer queued, so the stream's length is the number of jobs
// not yet taken.
type Queue struct {
	rdb      redis.Cmdable
	prefix   string
	consumer string
}

const queueGroup = "dispatch"

func NewQueue(rdb redis.Cmdable, prefix, consumer string) *Queue {
	if prefix == "" {
		prefix = "phoebe:videos:q:"
	}
	return &Queue{rdb: rdb, prefix: prefix, consumer: consumer}
}

func (q *Queue) key(resourceID string) string { return q.prefix + resourceID }

// Add appends a job id to its endpoint's stream.
func (q *Queue) Add(ctx context.Context, resourceID, jobID string) error {
	return q.rdb.XAdd(ctx, &redis.XAddArgs{Stream: q.key(resourceID), Values: map[string]any{"job": jobID}}).Err()
}

func (q *Queue) ensureGroup(ctx context.Context, resourceID string) error {
	err := q.rdb.XGroupCreateMkStream(ctx, q.key(resourceID), queueGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Entry is one stream entry.
type Entry struct {
	StreamID string
	JobID    string
}

// Next returns the endpoint's oldest entry this consumer may take: first one
// it read before but did not acknowledge (the endpoint was busy then), so a
// busy endpoint keeps its order; then entries left pending by a dispatcher
// that died more than idle ago; then a new one. nil when there is none.
func (q *Queue) Next(ctx context.Context, resourceID string, idle time.Duration) (*Entry, error) {
	if err := q.ensureGroup(ctx, resourceID); err != nil {
		return nil, err
	}
	key := q.key(resourceID)
	read := func(id string) (*Entry, error) {
		res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: queueGroup, Consumer: q.consumer, Streams: []string{key, id}, Count: 1, Block: -1,
		}).Result()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		for _, s := range res {
			for _, m := range s.Messages {
				return entryOf(m), nil
			}
		}
		return nil, nil
	}
	if e, err := read("0"); e != nil || err != nil {
		return e, err
	}
	msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: key, Group: queueGroup, Consumer: q.consumer, MinIdle: idle, Start: "0", Count: 1,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	if len(msgs) > 0 {
		return entryOf(msgs[0]), nil
	}
	return read(">")
}

func entryOf(m redis.XMessage) *Entry {
	id, _ := m.Values["job"].(string)
	return &Entry{StreamID: m.ID, JobID: id}
}

// Done acknowledges and deletes an entry.
func (q *Queue) Done(ctx context.Context, resourceID, streamID string) error {
	key := q.key(resourceID)
	if err := q.rdb.XAck(ctx, key, queueGroup, streamID).Err(); err != nil {
		return err
	}
	return q.rdb.XDel(ctx, key, streamID).Err()
}

// Len is the number of entries not yet taken.
func (q *Queue) Len(ctx context.Context, resourceID string) (int64, error) {
	n, err := q.rdb.XLen(ctx, q.key(resourceID)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("xlen: %w", err)
	}
	return n, nil
}

// Ping checks Valkey is reachable.
func (q *Queue) Ping(ctx context.Context) error {
	return q.rdb.Ping(ctx).Err()
}
