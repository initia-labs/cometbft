---
order: 1
parent:
  title: Sequencing
  order: false
---

# Sequencing

The sequencing subsystem governs how proposed blocks, attestations, and other
rollup-specific inputs flow through the node before they become part of the
canonical chain. This directory hosts implementation-oriented specifications
for the primary coordination structures used by the sequencing engine.

- [Data Structures](./data_structures.md): queueing, peer grading, and request
  tracking primitives used by the sequencing engine processors.
- [Engine Overview](./engine.md): lifecycle, processors, and message flow for
  the sequencing engine.
