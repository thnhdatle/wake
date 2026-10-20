// SPDX-License-Identifier: Apache-2.0

package redisl2

import "github.com/redis/go-redis/v9"

// prelude is shared by every script that needs the server clock or a
// generation. It reads TIME once; fmt formats integers exactly ('%d', never
// %g); gen returns the generation of key k as a string, initializing a
// missing or unparsable one to TIME in microseconds and extending its TTL to
// at least ttl ms (never shortening it; a key without TTL keeps none); put
// writes the value record "<gen>|<expires_at_ms>|<payload>" with PX ttl.
// A ttl of 0 means no expiry, as in the l2 fake.
const prelude = `
local t = redis.call('TIME')
local now_us = tonumber(t[1]) * 1000000 + tonumber(t[2])
local now_ms = math.floor(now_us / 1000)
local function fmt(n) return string.format('%d', n) end
local function gen(k, ttl)
  local g = redis.call('GET', k)
  if g and string.match(g, '^%d+$') then
    local p = redis.call('PTTL', k)
    if p >= 0 and p < ttl then redis.call('PEXPIRE', k, ttl) end
    return g
  end
  g = fmt(now_us)
  if ttl > 0 then redis.call('SET', k, g, 'PX', ttl) else redis.call('SET', k, g) end
  return g
end
local function put(k, g, payload, ttl)
  local rec = g .. '|' .. fmt(now_ms + ttl) .. '|' .. payload
  if ttl > 0 then redis.call('SET', k, rec, 'PX', ttl) else redis.call('SET', k, rec) end
end
local function epoch(k)
  local e = redis.call('GET', k)
  if e and string.match(e, '^%d+$') then return e end
  e = fmt(now_us)
  redis.call('SET', k, e)
  return e
end
`

// readScript: KEYS value, gen; ARGV gen_ttl_ms.
// Returns {gen} on a miss and {gen, ttl_ms, payload} on a hit: a value whose
// generation string equals the current one and whose expires_at is ahead of
// TIME.
const readScript = prelude + `
local g = gen(KEYS[2], tonumber(ARGV[1]))
local raw = redis.call('GET', KEYS[1])
if not raw then return {g} end
local i = string.find(raw, '|', 1, true)
if not i then return {g} end
local j = string.find(raw, '|', i + 1, true)
if not j or string.sub(raw, 1, i - 1) ~= g then return {g} end
local exp = string.sub(raw, i + 1, j - 1)
if not string.match(exp, '^%-?%d+$') then return {g} end
exp = tonumber(exp)
if exp <= now_ms then return {g} end
return {g, fmt(exp - now_ms), string.sub(raw, j + 1)}
`

// fillScript: KEYS value, gen, lock; ARGV gen, payload, ttl_ms, gen_ttl_ms,
// lock token (empty for none), channel, loaded message.
// Writes the value only if the generation still equals ARGV gen; deletes
// the lock if it holds the token; publishes "loaded" if it did either.
// Returns 1 when the value was stored.
const fillScript = prelude + `
local stored = 0
if gen(KEYS[2], tonumber(ARGV[4])) == ARGV[1] then
  put(KEYS[1], ARGV[1], ARGV[2], tonumber(ARGV[3]))
  stored = 1
end
local released = false
if ARGV[5] ~= '' and redis.call('GET', KEYS[3]) == ARGV[5] then
  redis.call('DEL', KEYS[3])
  released = true
end
if stored == 1 or released then redis.call('PUBLISH', ARGV[6], ARGV[7]) end
return stored
`

// setScript: KEYS value, gen; ARGV payload, ttl_ms, gen_ttl_ms, channel,
// message prefix "i|<epoch>|", key.
// Increments the generation, writes the value with it and publishes
// "i|<epoch>|<gen>|<key>". Returns the new generation.
const setScript = prelude + `
gen(KEYS[2], tonumber(ARGV[3]))
local g = fmt(redis.call('INCR', KEYS[2]))
put(KEYS[1], g, ARGV[1], tonumber(ARGV[2]))
redis.call('PUBLISH', ARGV[4], ARGV[5] .. g .. '|' .. ARGV[6])
return g
`

// invalidateScript: KEYS value, gen; ARGV gen_ttl_ms, channel, message
// prefix "i|<epoch>|", key.
// Increments the generation, deletes the value and publishes
// "i|<epoch>|<gen>|<key>". Returns the new generation.
const invalidateScript = prelude + `
gen(KEYS[2], tonumber(ARGV[1]))
local g = fmt(redis.call('INCR', KEYS[2]))
redis.call('DEL', KEYS[1])
redis.call('PUBLISH', ARGV[2], ARGV[3] .. g .. '|' .. ARGV[4])
return g
`

// unlockScript: KEYS lock; ARGV token, channel, loaded message.
// Compare-and-delete; publishes "loaded" only when it released the lock.
const unlockScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  redis.call('PUBLISH', ARGV[2], ARGV[3])
  return 1
end
return 0
`

// epochScript: KEYS epoch. Returns the epoch, initializing a missing (or
// unparsable) key to TIME in microseconds, without TTL.
const epochScript = prelude + `
return epoch(KEYS[1])
`

// bumpEpochScript: KEYS epoch; ARGV channel.
// Initializes a missing epoch like epochScript, increments it and publishes
// "e|<epoch>". Returns the new epoch.
const bumpEpochScript = prelude + `
epoch(KEYS[1])
local e = fmt(redis.call('INCR', KEYS[1]))
redis.call('PUBLISH', ARGV[1], 'e|' .. e)
return e
`

// scripts holds one Backend's scripts (source and SHA-1 digest).
type scripts struct {
	read, fill, set, invalidate, unlock, epoch, bumpEpoch *redis.Script
}

func newScripts() scripts {
	return scripts{
		read:       redis.NewScript(readScript),
		fill:       redis.NewScript(fillScript),
		set:        redis.NewScript(setScript),
		invalidate: redis.NewScript(invalidateScript),
		unlock:     redis.NewScript(unlockScript),
		epoch:      redis.NewScript(epochScript),
		bumpEpoch:  redis.NewScript(bumpEpochScript),
	}
}
