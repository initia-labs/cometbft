---
title: Sequencing Data Structures
order: 2
---

# Sequencing Data Structures

This document captures the contractual behaviour of the sequencing engine data
structures that coordinate cross-peer block flow: the peer bucket, peer set,
and request tracker. These types live under `sequencing/types` and are shared
across processors inside the sequencing engine.

## Common Expectations

- All structures are concurrency-safe; exported methods tolerate concurrent
  callers and use internal synchronisation to protect state.
- Heights refer to CometBFT block heights. When conflicts arise, lower heights
  take precedence to preserve determinism for replays.
- None of the structures perform logging; callers must log when reacting to
  their return values.

## `P2PBucket`

`P2PBucket[T]` multiplexes height-ordered queues per peer and exposes the
lowest-height entry across all peers. The sequencing engine keeps separate
instances for proposed blocks and attestor commits.

### Invariants

- Entries for each peer are strictly ordered by ascending height. Duplicate
  heights per peer are allowed, but their relative order is the arrival order.
- The heap of peer fronts always references the minimum height for each peer
  still present in the bucket.
- `SELF_PEER_ID` designates queue entries produced locally; it is treated like
  any other peer identifier and subject to the same ordering rules.

### Operations

- `Add(id, height, value)` inserts a value into the peer queue. The operation
  keeps the peer slice sorted via binary search insertion and updates the heap
  when the peer's front height changes. Complexity is `O(log n + m)` where `n`
  is the number of peers and `m` the queue length for `id`.
- `PeekLowest()` returns `(peer, height, value, true)` for the globally lowest
  entry without mutating the bucket. When empty it returns `(_, _, zero, false)`.
- `PopLowest()` removes the globally lowest entry, re-inserting the owning peer
  into the heap if it has additional elements. Returns the same tuple as
  `PeekLowest()`.
- `RemovePeer(id)` drops all queued items for `id`, returns `true` when the peer
  existed, and updates the heap accordingly.
- `HasPeer(id)` and `PeerLen(id)` are fast membership/length probes that do not
  alter ordering and are safe under read locks.
- `HasHeight(height)` reports whether any peer has queued the target height. It
  performs a binary search per peer and returns as soon as a match is found.
- `Remove(height)` deletes exactly one entry with the target height (searching
  peer queues by binary search) and returns its `(peer, value, true)` tuple.

### Failure Modes

- Calling `PopLowest()` on an empty bucket returns `(_, 0, zero, false)`. The
  same sentinel applies to `PeekLowest()` and `Remove(height)`.
- Removing a peer that is not present returns `false` and leaves the heap
  unchanged.

## `PeerSet`

`PeerSet` tracks the advertised availability of peers, a smoothed performance
grade, and request state used to balance outbound block fetches.

### Invariants

- Each peer keeps an inclusive `[baseHeight, latestHeight]` window describing
  blocks it can serve. If the caller supplies `baseHeight > latestHeight`, the
  values are swapped.
- Grades default to `1.0` and are smoothed using
  `grade = (1-α)*grade + α*(1/latency)` with `α = 0.2`. Grades never drop below
  `1e-3`.
- Pending requests are keyed by height; only one active pending entry per
  `(peer, height)` pair exists at a time.

### Operations

- `Update(id, base, latest)` inserts or refreshes a peer and resets its height
  window and grade bookkeeping.
- `Remove(id)` deletes the peer and associated request/timeout metadata.
- `Peers(height, k)` returns up to `k` peers sorted by descending grade that can
  serve `height`. Ties are broken by lexical order of `p2p.ID` to keep ordering
  stable.
- `PeersForRequest(height, limit, now, timeout)` returns three values:
  1. Peers ready for selection (no active request) sorted by grade;
  2. Peers whose previous requests expired during this call;
  3. The total number of peers that advertise the height regardless of request
     state.
  Pending requests older than `timeout` are cleared before candidate selection.
  The number of returned peers respects `limit` minus the count of still-active
  pending requests for the same height.
- `HasActiveRequest(height, now, timeout)` reports whether any peer still has an
  in-flight request that has not exceeded `timeout`. Expired entries are purged
  before the check completes.
- `RecordRequest(id, height, ts, timeout)` notes that a request was dispatched.
  It records `ts` and, when `timeout > 0`, arms a background timer to expire the
  request automatically. Re-recording for the same `(peer, height)` overwrites
  the previous timestamp.
- `RecordResponse(id, height, ts)` clears the pending entry and updates the
  peer's grade using the response latency. Latency is clamped to
  `minLatencyScore` to avoid division by zero before computing `1/latency`.

### Expiration and Timeouts

- Expired requests reduce the peer's grade by multiplying it by `(1-α)` before
  the minimum grade clamp is applied.
- The timed-out peer ID is returned once via `PeersForRequest` and cleared so
  callers can react to the timeout exactly once.
- Timer callbacks (`time.AfterFunc`) run under the same lock as synchronous
  operations and call `expireRequest` to reuse the same expiration logic.

## `BlockRequestTracker`

`BlockRequestTracker` enforces a global cap on concurrent block fetches per
node. It complements `PeerSet` by tracking heights rather than peer-specific
assignments.

### Behaviour

- The tracker is created with a maximum capacity and a TTL (`ttl`). When `ttl`
  is non-positive, reservations never auto-expire.
- `TryReserve(height, now)` first garbage-collects expired reservations then
  attempts to reserve `height`. It returns `false` if the height is already
  pending or if capacity has been reached; otherwise it stores `now` and
  returns `true`.
- `Release(height)` frees a reservation regardless of its age.
- `Active(now)` returns the count of current reservations after running the
  same TTL-based garbage collection as `TryReserve`.
- `GC(now)` forces garbage collection without returning the active count.

### Garbage Collection

- Expiration only removes reservations whose age is strictly greater than `ttl`
  (`now - ts > ttl`). Callers should pass consistent timestamps to avoid
  surprising dropouts during long pauses.
- Garbage collection takes place while holding the tracker lock; callers must
  avoid invoking `GC` from contexts that already hold it.

## Interaction Overview

Together, these structures allow the sequencing engine to assign block fetches
to peers while avoiding duplicate work and overloading any single peer:

1. Incoming block announcements are enqueued via `P2PBucket.Add` and polled by
   processors using `PeekLowest`/`PopLowest` to enforce height ordering.
2. Before dispatching a fetch, the engine consults `PeerSet.PeersForRequest` and
   `BlockRequestTracker.TryReserve` to confirm both peer availability and global
   capacity.
3. When responses arrive, the engine updates the peer grade with
   `RecordResponse`, removes the associated bucket entry, and releases the height
   in the request tracker.

These guarantees ensure deterministic ordering, graceful handling of slow or
unresponsive peers, and bounded concurrency across the sequencing pipeline.
