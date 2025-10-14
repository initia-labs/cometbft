---
order: 1
parent:
  title: Sequencing
  order: false
---

# Sequencing

The sequencing subsystem governs how proposed blocks, attestations, and other
rollup-specific inputs flow through the node before they become part of the
canonical chain. It replaces the classic Tendermint/Comet consensus loop with
a purpose-built reactor that is optimized for L2 rollups.

## Sequencing Reactor: Overview & Rationale

- Replaces legacy consensus and block sync with a specialized Sequencing
  Reactor. Blocks advance as soon as the sequencer signs; attestor signatures
  are collected asynchronously and merged later.
- Reuses Comet voting power with clear roles and weights: sequencer = 1,
  attestor = 3. Light clients (for example, IBC) still require a >2/3 attestor
  quorum, but execution never waits for it.
- Wires node, mempool, RPC, and store to the new flow; adds specs, tests,
  metrics, and validator-set persistence/caching.

### Why this matters

- Fast blocks with a deterministic leader: a single sequencer can push the
  chain forward immediately after signing.
- Safety for light clients: attestor quorum remains required for external
  verification, just not for execution.
- Less complexity: the system drops the full Tendermint/Comet consensus
  reactor for this L2 use case, reducing moving parts to operate and tune.

## Roles, Voting Power, and Quorum

### Sequencer

- Voting power: `1`
- Duty: propose and sign the block so the chain can execute immediately.

### Attestors

- Voting power: `3` each
- Duty: sign blocks asynchronously; their signatures are merged later to form
  an IBC-usable commit.

With one sequencer and three attestors, total power is `10`. A valid IBC
commit (>2/3) needs `7`, which means the sequencer (1) plus at least two
attestors (3 + 3). The sequencer cannot finalize alone but keeps production
unblocked.

## Block Lifecycle (Happy Path)

1. **Propose & Execute (hot path)**  
   The sequencer proposes the block and signs. The engine verifies only the
   sequencer commit (`VerifySequencerCommit`) and applies the block
   immediately.
2. **Async Attestation (off the hot path)**  
   Attestors sign in the background. Reactor processors merge extended commits
   (attestor signatures) into the block store as they arrive.
3. **IBC Relay**  
   Once the stored commit shows ≥2/3 attestor power, an IBC relayer can relay
   a light-client update. Production never waited on this step.

## What Changed (Developer-Facing)

- **New subsystem**: reactor and engine under `sequencing/reactor.go` and
  `sequencing/engine/*` introduce lifecycle management, peer coordination, and
  message channels (Propose / Attest / Sync). Specs live under
  `spec/sequencing/*`.
- **Validation & voting power**: updates in `types/validation.go` and
  `types/validator_set.go` add sequencer/attestor constants, enforce a single
  sequencer (`EnsureSingleSequencer`), and expose `VerifySequencerCommit`.
  
- **State & store**: `state/execution.go` and `state/validation.go` enforce the
  single sequencer and verify sequencer commits on the last commit. The block
  store caches and persists validator sets alongside blocks.
- **Mempool**: per-peer intake moves check-tx load off the hot path to improve
  throughput.
- **RPC**: status and catch-up paths understand the sequencing reactor. Classic
  consensus RPCs return errors when consensus is absent. `Commit` returns the
  seen commit with `full=true`.
- **Docs**: `spec/sequencing/engine.md` adds IBC attestor coordination and
  voting-power weights.

## Operational Model

- **Who can propose?** The single sequencer (power = 1). Any node with attestor
  power (power = 3) signs asynchronously when it observes a new block.
- **What does “async” mean?** Attestor processors and signers run independently;
  they do not gate `FinalizeBlock` or `Commit`. Blocks continue to flow even if
  some attestors are slow or offline. IBC readiness trails until ≥2/3 attestor
  power arrives.
- **How do peers catch up?** The engine ships sync channels, request trackers,
  and background processors to fetch, buffer, and apply blocks; it continually
  reconciles commits received from peers.

## Benefits

- **Latency**: blocks finalize for execution as soon as the sequencer signs
  without waiting for quorum.
- **Reliability**: attestor lag does not stall production; signatures can
  trickle in after execution.
- **Simplicity**: a purpose-built reactor reduces consensus complexity for L2.
- **Throughput**: per-peer mempool validation and validator-set caching avoid
  bottlenecks.
- **IBC friendliness**: clear quorum math (1/3 weights) and eventual complete
  commit sets for light clients.

## Configuration & Integration Notes

- **Config**: `Config.Sequencing` steers node wiring toward sequencing instead
  of consensus/block-sync in L2 mode.
- **RPC clients**: tools expecting classic consensus RPCs should handle
  “consensus disabled” errors or switch to sequencing-aware status paths.
- **Relayers**: monitor the block-store commit for ≥2/3 attestor power before
  relaying client updates or proofs. Production height can outpace IBC-ready
  finality.

## Edge Cases & Safeguards

- Single sequencer enforced via `EnsureSingleSequencer()` during state updates.
- Execution verifies only the sequencer commit (`VerifySequencerCommit`);
  attestor commits merge later through processors.
- Status APIs should reflect reactor catch-up state to avoid misreporting sync
  status; this remains an area for continued validation.

## FAQ

**Does attestation affect block production?**  
No. Attestation is asynchronous; block production and application proceed as
soon as the sequencer signs.

**When can the IBC relayer act?**  
Once the stored commit contains ≥2/3 attestor power (plus the sequencer).

**Why give the sequencer only 1 power?**  
The sequencer signature is required to advance execution but insufficient to
finalize for light clients—attestors still control finality.

## Further Reading

- [Data Structures](./data_structures.md): queueing, peer grading, and request
  tracking primitives used by sequencing engine processors.
- [Engine Overview](./engine.md): lifecycle, processors, and message flow for
  the sequencing engine.
