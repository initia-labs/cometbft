package engine

import (
	"time"
)

const (
	syncInterval    = 10 * time.Millisecond
	proposeInterval = 50 * time.Millisecond
	attestInterval  = 100 * time.Millisecond
	statusInterval  = 2 * time.Second
)

func (e *Engine) blockProcessor() {
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for {
				select {
				case <-e.stopCh:
					return
				default:
				}

				e.stateMu.Lock()
				stateHeight := e.state.LastBlockHeight
				e.stateMu.Unlock()

				// try to keep a buffer of future blocks
				e.requestFutureBlocks(stateHeight)

				pid, height, proposedBlock, ok := e.blockBucket.PopLowest()
				if !ok {
					break
				}

				if height <= stateHeight {
					e.requestWindow.Release(height)
					e.blockBucket.Remove(height)
					continue
				} else if height > stateHeight+1 {
					// future block, put it back and wait for the next tick
					e.blockBucket.Add(pid, height, proposedBlock)
					e.requestWindow.Release(height)

					// need to request the missing blocks
					if !e.requestBlock(stateHeight + 1) {
						e.requestWindow.Release(stateHeight + 1)
					}

					break
				}

				// only process blocks that are for the current height
				// or the next height (in case we missed the previous block)
				// otherwise put it back and wait for the next tick

				e.logger.Debug("block processor popped block", "peer", pid, "height", height)

				// try to register event bus after we receive block
				e.tryRegisterEventBus()

				if badPeer, applied, upgrade := e.applyProposedBlock(proposedBlock); upgrade {
					return
				} else if badPeer {
					e.flagBadPeer(pid, "sent invalid proposed block")
				} else if applied {
					e.logger.Info("applied proposed block", "peer", pid, "height", height)

					// Only rebroadcast when the message carried provenance (i.e. not a direct reply).
					if proposedBlock.PeerFilter != nil {
						e.broadcastProposedBlock(proposedBlock)
					}
				} else if !applied && proposedBlock != nil && proposedBlock.Commit != nil {
					// If the block was not applied, it may be because we are on a different fork.
					e.enqueueConflictingCommit(pid, proposedBlock.Commit.ToCommit())
				}

				e.requestWindow.Release(height)
			}

		case <-e.stopCh:
			return
		}
	}
}

func (e *Engine) attesterCommitProcessor() {
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for {
				select {
				case <-e.stopCh:
					return
				default:
				}

				id, height, attestorCommit, ok := e.commitBucket.PopLowest()
				if !ok {
					break
				}

				if height > e.blockStore.Height() {
					e.commitBucket.Add(id, height, attestorCommit)
					break
				}

				badPeer, applied := e.applyAttestorCommit(attestorCommit)
				if badPeer {
					e.flagBadPeer(id, "sent invalid attestor commit")
					continue
				}
				if applied {
					e.logger.Info("applied attestor commit", "peer", id, "height", height)

					// Only rebroadcast when the message carried provenance (i.e. not a direct reply).
					if attestorCommit.PeerFilter != nil {
						e.broadcastAttestorCommit(attestorCommit)
					}
				}
			}

		case <-e.stopCh:
			return
		}
	}
}

func (e *Engine) statusProcessor() {
	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.broadcastStatus()
		case <-e.stopCh:
			return
		}
	}
}

func (e *Engine) proposerProcessor() {
	ticker := time.NewTicker(proposeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.proposeBlock()
		case <-e.appliedCh:
			e.proposeBlock()
		case <-e.reactor.TxsAvailable():
			e.proposeBlock()
		case <-e.stopCh:
			return
		}
	}
}

func (e *Engine) attestorProcessor() {
	ticker := time.NewTicker(attestInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.attestBlock()
		case <-e.appliedCh:
			e.attestBlock()
		case <-e.stopCh:
			return
		}
	}
}

func (e *Engine) conflictingVoteProcessor() {
	for {
		select {
		case <-e.stopCh:
			return
		case item := <-e.conflictingVotesCh:
			if item.commit != nil {
				e.checkConflictingVotes(item.peerID, item.commit)
			}
		}
	}
}

func (e *Engine) badPeerCleanup() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			e.badPeers.Range(func(key, value any) bool {
				badPeerTime := value.(time.Time)
				if now.Sub(badPeerTime) > 30*time.Second {
					e.badPeers.Delete(key)
				}
				return true
			})
		case <-e.stopCh:
			return
		}
	}
}
