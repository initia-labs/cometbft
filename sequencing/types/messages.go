package types

import (
	"fmt"

	"github.com/cometbft/cometbft/p2p"
	seqproto "github.com/cometbft/cometbft/proto/tendermint/sequencing"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
)

// Message is the generic interface implemented by every sequencing message.
type Message interface {
	ProtoMessage() *seqproto.Message
}

type ProposedBlock struct {
	Block   *types.Block
	Commit  *types.ExtendedCommit
	PeerIDs []p2p.ID
}

func ProposedBlockFromProto(pb *seqproto.ProposedBlock, peerIDs []p2p.ID) (*ProposedBlock, error) {
	if pb == nil {
		return nil, fmt.Errorf("nil ProposedBlock proto")
	}
	block, err := types.BlockFromProto(pb.Block)
	if err != nil {
		return nil, err
	}
	extCommit, err := types.ExtendedCommitFromProto(pb.Commit)
	if err != nil {
		return nil, err
	}
	return &ProposedBlock{
		Block:   block,
		Commit:  extCommit,
		PeerIDs: peerIDs,
	}, nil
}

type AttestorCommit struct {
	Commit  *types.ExtendedCommit
	PeerIDs []p2p.ID
}

func AttestorCommitFromProto(pb *seqproto.AttestorCommit, peerIDs []p2p.ID) (*AttestorCommit, error) {
	if pb == nil {
		return nil, fmt.Errorf("nil AttestorCommit proto")
	}
	commit, err := types.ExtendedCommitFromProto(pb.Commit)
	if err != nil {
		return nil, err
	}
	return &AttestorCommit{Commit: commit, PeerIDs: peerIDs}, nil
}

type StatusUpdate struct {
	BaseHeight int64
	LastHeight int64
}

func (m *StatusUpdate) ProtoMessage() *seqproto.Message {
	return &seqproto.Message{Sum: &seqproto.Message_StatusUpdate{StatusUpdate: &seqproto.StatusUpdate{
		BaseHeight: m.BaseHeight,
		LastHeight: m.LastHeight,
	}}}
}

func StatusUpdateFromProto(pb *seqproto.StatusUpdate) *StatusUpdate {
	if pb == nil {
		return nil
	}
	return &StatusUpdate{BaseHeight: pb.BaseHeight, LastHeight: pb.LastHeight}
}

type BlockRequest struct {
	Height int64
}

func (m *BlockRequest) ProtoMessage() *seqproto.Message {
	return &seqproto.Message{Sum: &seqproto.Message_BlockRequest{BlockRequest: &seqproto.BlockRequest{
		Height: m.Height,
	}}}
}

func BlockRequestFromProto(pb *seqproto.BlockRequest) *BlockRequest {
	if pb == nil {
		return nil
	}
	return &BlockRequest{Height: pb.Height}
}

type BlockResponse struct {
	ProposedBlock  *ProposedBlock
	AttesterCommit *AttestorCommit
	PeerIDs        []p2p.ID
}

func (m *BlockResponse) ProtoMessage() *seqproto.Message {
	br := &seqproto.BlockResponse{}
	if m.ProposedBlock != nil {
		br.ProposedBlock = &seqproto.ProposedBlock{
			Block:  mustBlockToProto(m.ProposedBlock.Block),
			Commit: m.ProposedBlock.Commit.ToProto(),
		}
	}
	if m.AttesterCommit != nil {
		br.AttesterCommit = &seqproto.AttestorCommit{
			Commit: m.AttesterCommit.Commit.ToProto(),
		}
	}
	if len(m.PeerIDs) > 0 {
		peerIDs := make([]string, len(m.PeerIDs))
		for i, pid := range m.PeerIDs {
			peerIDs[i] = string(pid)
		}
		br.PeerIds = peerIDs
	}
	return &seqproto.Message{Sum: &seqproto.Message_BlockResponse{BlockResponse: br}}
}

func BlockResponseFromProto(pb *seqproto.BlockResponse) (*BlockResponse, error) {
	if pb == nil {
		return nil, fmt.Errorf("nil BlockResponse proto")
	}
	var peerIDs []p2p.ID
	if len(pb.PeerIds) > 0 {
		peerIDs = make([]p2p.ID, len(pb.PeerIds))
		for i, pid := range pb.PeerIds {
			peerIDs[i] = p2p.ID(pid)
		}
	}
	var proposedBlock *ProposedBlock
	var err error
	if pb.ProposedBlock != nil {
		proposedBlock, err = ProposedBlockFromProto(pb.ProposedBlock, peerIDs)
		if err != nil {
			return nil, err
		}
	}
	var attesterExtCommit *AttestorCommit
	if pb.AttesterCommit != nil {
		attesterExtCommit, err = AttestorCommitFromProto(pb.AttesterCommit, peerIDs)
		if err != nil {
			return nil, err
		}
	}
	return &BlockResponse{ProposedBlock: proposedBlock, AttesterCommit: attesterExtCommit, PeerIDs: peerIDs}, nil
}

func MsgFromProto(msg *seqproto.Message) (Message, error) {
	if msg == nil {
		return nil, fmt.Errorf("empty sequencing msg")
	}
	switch payload := msg.Sum.(type) {
	case *seqproto.Message_StatusUpdate:
		return StatusUpdateFromProto(payload.StatusUpdate), nil
	case *seqproto.Message_BlockRequest:
		return BlockRequestFromProto(payload.BlockRequest), nil
	case *seqproto.Message_BlockResponse:
		return BlockResponseFromProto(payload.BlockResponse)
	default:
		return nil, fmt.Errorf("unknown sequencing msg type %T", payload)
	}
}

func MsgToProto(msg Message) *seqproto.Message {
	if msg == nil {
		return nil
	}
	return msg.ProtoMessage()
}

func mustBlockToProto(block *types.Block) *cmtproto.Block {
	if block == nil {
		return nil
	}
	proto, err := block.ToProto()
	if err != nil {
		panic(fmt.Errorf("convert block to proto: %w", err))
	}
	return proto
}
