package engine

import (
	"time"
)

const (
	syncInterval    = 10 * time.Millisecond
	proposeInterval = 100 * time.Millisecond
	attestInterval  = 100 * time.Millisecond
	statusInterval  = 2 * time.Second
)

func (e *Engine) blockProcessor() {
	ticker := time.NewTicker(syncInterval)
	for {
		select {
		case <-ticker.C:
			for {
				e.stateMu.Lock()
				stateHeight := e.state.LastBlockHeight
				e.stateMu.Unlock()

				// try to keep a buffer of future blocks
				e.requestFutureBlocks(stateHeight)

				id, height, proposedBlock, ok := e.blockBucket.PopLowest()
				if !ok {
					break
				}

				if height <= stateHeight {
					e.requestWindow.Release(height)
					e.blockBucket.Remove(height)
					continue
				} else if height > stateHeight+1 {
					// future block, put it back and wait for the next tick
					e.blockBucket.Add(id, height, proposedBlock)
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

				e.logger.Debug("block processor popped block", "peer", id, "height", height)

				// try to register event bus after we receive block
				e.tryRegisterEventBus()

				if badPeer, applied := e.applyProposedBlock(proposedBlock); badPeer {
					e.flagBadPeer(id, "sent invalid proposed block")
				} else if applied {
					e.logger.Info("applied proposed block", "peer", id, "height", height)

					// Only rebroadcast when the message carried provenance (i.e. not a direct reply).
					if proposedBlock.PeerFilter != nil {
						e.broadcastProposedBlock(proposedBlock)
					}
				}

				e.requestWindow.Release(height)
			}

		case <-e.stopCh:
			ticker.Stop()
			return
		}
	}
}

func (e *Engine) attesterCommitProcessor() {
	ticker := time.NewTicker(syncInterval)
	for {
		select {
		case <-ticker.C:
			for {
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
			ticker.Stop()
			return
		}
	}
}

func (e *Engine) statusProcessor() {
	ticker := time.NewTicker(statusInterval)
	for {
		select {
		case <-ticker.C:
			e.broadcastStatus()
		case <-e.stopCh:
			ticker.Stop()
			return
		}
	}
}

func (e *Engine) proposerProcessor() {
	ticker := time.NewTicker(proposeInterval)
	for {
		select {
		case <-ticker.C:
			e.proposeBlock()
		case <-e.appliedCh:
			e.proposeBlock()
		case <-e.reactor.TxsAvailable():
			e.proposeBlock()
		case <-e.stopCh:
			ticker.Stop()
			return
		}
	}
}

func (e *Engine) attestorProcessor() {
	ticker := time.NewTicker(attestInterval)
	for {
		select {
		case <-ticker.C:
			e.attestBlock()
		case <-e.appliedCh:
			e.attestBlock()
		case <-e.stopCh:
			ticker.Stop()
			return
		}
	}
}

func (e *Engine) badPeerCleanup() {
	ticker := time.NewTicker(10 * time.Second)
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
			ticker.Stop()
			return
		}
	}
}
