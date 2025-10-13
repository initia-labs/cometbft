---
title: Sequencing Engine Overview
order: 3
---

# Sequencing Engine Overview

The sequencing engine orchestrates the intake of network messages, local block
production, attestation duties, and block execution for a CometBFT node running
with sequencing roles enabled. This document describes the core loop
structure, supporting components, and interactions between processors.

## Runtime Structure

The engine is instantiated by `NewEngine` in `sequencing/engine`. It wires
dependencies from configuration, state, execution, and p2p layers, and creates
the data structures documented in [Data Structures](./data_structures.md).

`Engine.Start` launches five long-lived goroutines, each driven by a periodic
`time.Ticker` and cancellation via `stopCh`:

1. **Block Processor** (`blockProcessor`): consumes proposed blocks from the
   block bucket, executing them in height order.
2. **Attestor Commit Processor** (`attesterCommitProcessor`): merges incoming
   attestor commits into the stored seen commit set.
3. **Status Processor** (`statusProcessor`): periodically rebroadcasts the
   node's status to peers.
4. **Proposer Processor** (`proposerProcessor`): generates proposals when the
   node is the sequencer and local conditions allow.
5. **Attestor Processor** (`attestorProcessor`): produces attestor precommits
   for the latest block when the node holds attestor voting power.

All processors terminate when `Engine.Stop` closes `stopCh`.

## Message Intake

Network messages enter through `Engine.Receive`, which filters out peers on the
`badPeers` list and dispatches by concrete message type:

- `StatusUpdate` → `handleStatusUpdate`
- `BlockRequest` → `handleBlockRequest`
- `BlockResponse` → `handleBlockResponse`

These handlers interact with the peer set, request tracker, and buckets to
update peer metadata, satisfy requests, or enqueue new blocks/commits.

## Block Processing Flow

The block processor loop performs the following steps on each tick:

1. Query the current executed height under `stateMu`.
2. Call `requestFutureBlocks` to ensure the request window maintains a buffer
   of upcoming blocks.
3. Drain the lowest entry from `blockBucket`.
4. Filter out stale or excessively future blocks, requeuing and requesting
   missing predecessors as needed.
5. Validate and execute the block via `applyProposedBlock`.
6. Release the height from `requestWindow` regardless of outcome.

`applyProposedBlock` validates commits against the sequencer validator set,
runs `blockExec.ApplyVerifiedBlock`, persists the block, updates tracked state
metrics (`lastProposedBlock*`), and signals `appliedCh` to wake proposer and
attestor processors. Bad blocks cause the sender to be flagged via `flagBadPeer`.

## Attestor Commit Flow

`attesterCommitProcessor` ensures the blockstore has the relevant block and
validator set before merging signatures. For each signature in an incoming
`AttestorCommit`, the processor verifies the signature, validator index, and
public key, updating the stored extended commit if additional signatures are
accepted. Invalid signatures cause the peer to be flagged. Successful merges
optionally trigger rebroadcast when the message carried provenance metadata.

## Proposer Responsibilities

The proposer processor monitors three triggers: ticker interval, block
application via `appliedCh`, and the reactor's `TxsAvailable` channel. On each
trigger it:

1. Checks whether enough time has elapsed since the last proposal and whether
   empty block throttling permits a new proposal.
2. Verifies the node currently has sequencer voting power.
3. Builds a proposal using `blockExec.CreateProposalBlock`, basing the commit
   on the stored seen commit for the previous height.
4. Signs its own vote and embeds it in the commit skeleton.
5. Enqueues the proposal into `blockBucket` under `SELF_PEER_ID` and broadcasts
   it to peers.
6. Updates `lastProposedBlockHeight` to suppress duplicate proposals.

## Attestor Responsibilities

The attestor processor reacts to the same ticker and `appliedCh` triggers. When
the node holds attestor voting power for the latest block height, it:

1. Loads the block and validator set from the blockstore.
2. Ensures the local seen commit has the expected shape, extending it if needed.
3. Signs a precommit vote and inserts it into the correct index.
4. Saves the updated commit and broadcasts an `AttestorCommit` message.

## IBC Attestor Coordination

IBC light clients require a full two-thirds quorum from the attestor committee,
but the sequencing engine keeps that requirement off the critical path for block
execution. `applyProposedBlock` validates only the sequencer's signature via
`VerifySequencerCommit`, so the block processor can advance as long as the
sequencer signs. Attestor signatures are merged later by
`attesterCommitProcessor`, which continually reconciles extended commits from
the network into the block store, and by the local `attestorProcessor`, which
re-signs the latest block whenever the node is an attestor. Because these loops
run independently from block proposal and application, sequencer-driven block
generation never waits on IBC attestations, while light clients still see an
eventually-complete commit set for finality proofs.

### Voting Power Weights

Sequencer validators are assigned voting power `1`, while each attestor carries
voting power `3` (`types/validation.go`). With one sequencer and three attestors
this yields a total voting power of `10`, so a valid IBC commit (> 2/3 of the
set) must include the sequencer plus at least two attestors (1 + 3 + 3 = 7).
The weighting keeps the sequencer's confirmatory signature effectively neutral
in the quorum math—it cannot finalize a block alone—yet still lets the engine
execute blocks immediately once the sequencer signs.

## Request Coordination

Block fetches are coordinated by the trio of structures described in the data
structure spec:

- `PeerSet` tracks peer height ranges, performance grades, and request timers.
- `P2PBucket` queues proposed blocks and attestor commits per peer, ensuring
  height ordering.
- `BlockRequestTracker` caps concurrent heights requested from the network.

`requestBlock` (invoked by the block processor and message handlers) uses
`PeerSet.PeersForRequest` alongside `requestWindow.TryReserve` to pick peers
and heights while respecting concurrency limits. Responses call
`requestWindow.Release` to free capacity.

## Error Handling and Peer Management

Peers that cause validation failures are passed to `flagBadPeer`, which
removes them from `peerSet`, records them in `badPeers`, and logs the reason.
Subsequent messages from flagged peers are ignored. `clearBadPeer` allows
manual reset when needed.

## Shutdown Semantics

`Engine.Stop` closes `stopCh`, prompting each processor loop to stop its ticker
and exit. Long-running operations (e.g., block application) are expected to
check the stop channel between iterations; remaining goroutines terminate once
their current iteration completes.

## Interaction Summary

1. Network input updates peer metadata and queues new block/commit messages.
2. Proposer/attestor processors react to application events and local timers to
   produce outbound messages when the node is responsible for sequencing or
   attesting.
3. The block processor executes blocks in order, updating state and signalling
   downstream processors.
4. Request coordination ensures duplicate work is avoided and misbehaving peers
   are isolated.

This architecture keeps the node responsive to both network-driven events and
local scheduling requirements while maintaining deterministic block execution.
