package groupcache

import (
	"time"
	log "github.com/Sirupsen/logrus" // todo: use log provider
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
		log.Debugf("object expired, key: %s, age: %d, expiration: %f seconds", key, age, g.expiration.Seconds())
		g.Stats.Expires.Add(1)
		loadErr := make(chan error)
		var backgroundBytes []byte
		backgroundDest := AllocatingByteSliceSink(&backgroundBytes)
		go func() {
			cacheWritten, err := g.loadAndPopulate(ctx, key, backgroundDest, true)
			if err == nil {
				// successfully load from local or peer
				cacheRead := which
				// cacheWritten有三种取值, Main / Hot / None
				// Main means load locally and cached in main cache
				// Hot means load from peer and cached in hot cache
				// None means load from peer and not cached in hot cache, or (TODO: exclude) get from cache when request intercepted by flight group
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
