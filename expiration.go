package groupcache

import (
	"time"
	"fmt"

	log "github.com/Sirupsen/logrus"
)

type TimeProvider interface {
	// Return the current Unix epoch time in seconds.
	NowUnixSeconds() int64

	// Return the time on a channel after delay d.
	After(d time.Duration) <-chan time.Time
}

type defaultTimeProvider struct{}

func (t defaultTimeProvider) NowUnixSeconds() int64 {
	return time.Now().Unix()
}

func (t defaultTimeProvider) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// Hook for testing.
var timeProvider TimeProvider = defaultTimeProvider{}

// Should only be set by testing code.
func setTimeProvider(tg TimeProvider) {
	timeProvider = tg
}

// GetTime should be called to produce a new timestamp to supply for
// Group.SetTimeStampBytes()
func GetUnixTime() int64 {
	return timeProvider.NowUnixSeconds()
}

// SetExpiration sets the cache expiration time. Internally the duration is
// truncated to seconds. If set to 0, will cache items forever.
func (g *Group) SetExpiration(d time.Duration) *Group {
	g.expiration = d
	return g
}

func (g *Group) SetLoadTimeout(d time.Duration) *Group {
	g.loadTimeout = d
	return g
}

func (g *Group) GetExpiration() time.Duration {
	return g.expiration
}

func (g *Group) GetLoadTimeout() time.Duration {
	return g.loadTimeout
}

// Clean cleans key in specific cache
func (g *Group) CleanUp(key string, which CacheType) {
	switch which {
	case HotCache:
		g.hotCache.remove(key)
	case MainCache:
		g.mainCache.remove(key)
	default:
		break
	}
}

// Evict evicts keys from either hot or main cache
func (g *Group) Evict(key string) {
	g.mainCache.remove(key)
	g.hotCache.remove(key)
}

// handleExpiration reloads object and populates dest if object is expired.
func (g *Group) handleExpiration(ctx Context, key string, dest Sink, value ByteView, which CacheType) error {
	writeTs, err := getTimestampByteView(value)
	if err != nil {
		return err
	}

	age := GetUnixTime() - writeTs
	// >=0 means expired
	if age - int64(g.expiration.Seconds()) >= 0 {
		log.Debugf("object expired, key: %s, age: %d, expiration: %v", key, age, g.expiration)
		g.Stats.Expires.Add(1)
		loadErr := make(chan error)
		var backgroundBytes []byte
		backgroundDest := AllocatingByteSliceSink(&backgroundBytes)
		go func() {
			cacheWritten, err := g.loadAndPopulate(ctx, key, backgroundDest, true)
			if err == nil {
				// successfully load from local or peer
				cacheRead := which
				// cacheWritten could be one of the following 3 values, Main / Hot / None;
				// Main means load locally and cached in main cache;
				// Hot means load from peer and cached in hot cache;
				// None means load from peer and not cached in hot cache, or (TODO: exclude) get from cache when request intercepted by flight group.
				if cacheWritten != cacheRead || cacheWritten == None {
					log.Debugf("cleaning group: %s, key: %s, value: %+v, age: %d, cacheRead: %v, cacheWritten: %v",
						g.Name(), key, value, age, cacheRead, cacheWritten)
					g.Stats.Cleanups.Add(1)
					g.CleanUp(key, cacheRead)
				}
			}
			loadErr <- err
		}()

		select {
		case err := <-loadErr:
			if err != nil {
				return err
			}
			bdView, _ := backgroundDest.view()
			return setSinkView(dest, bdView)
		case <-timeProvider.After(g.loadTimeout):
			break
		}
	}

	// Fall through for still cached or load timeout.
	return setSinkView(dest, value)
}

// Note: return expired kvs
func (g *Group) checkExpirationBatch(ctx Context, kvs []*KeyValue) ([]*KeyValue, error) {
	// partition kvs
	kvsExpired := []*KeyValue{}
	kvsUnexpired := []*KeyValue{}
	for _, kv := range kvs {
		writeTs, err := getTimestampByteView(kv.value)
		if err != nil {
			log.Errorf("[handleExpirationBatch] getTimestampByteView err: %v, value: %v", err.Error(), kv.value)
			kv.Err = fmt.Errorf("[handleExpirationBatch] getTimestampByteView failed, err: %s", err.Error())
			continue
		}
		age := GetUnixTime() - writeTs
		if age - int64(g.expiration.Seconds()) >= 0 {
			log.Debugf("object expired, key: %s, age: %ds, expiration: %v", kv.Key, age, g.expiration)
			g.Stats.Expires.Add(1)
			kvsExpired = append(kvsExpired, kv)
		} else {
			kvsUnexpired = append(kvsUnexpired, kv)
		}
	}

	// handle unexpired kvs
	for _, kv := range kvsUnexpired {
		if err := setSinkView(kv.Dest, kv.value); err != nil {
			log.Errorln("[handleExpirationBatch] setSinkView failed, err:", err)
			kv.Err = fmt.Errorf("[handleExpirationBatch] setSinkView failed, err: %s", err.Error())
		}
	}

	return kvsExpired, nil
}

func (g *Group) handleExpiredKeys(kvsExpired []*KeyValue) {
	for _, kv := range kvsExpired {
		if kv.whichRead != None && (kv.whichWrite != kv.whichRead || kv.whichWrite == None) {
			log.Debugf("cleaning group: %s, key: %s, value: %+v, cacheRead: %v, cacheWritten: %v",
				g.Name(), kv.Key, kv.value, kv.whichRead, kv.whichWrite)
			g.Stats.Cleanups.Add(1)
			g.CleanUp(kv.Key, kv.whichRead)
		}
	}
}


