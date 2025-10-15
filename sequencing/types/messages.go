package types

import (
	"fmt"

	seqproto "github.com/cometbft/cometbft/proto/tendermint/sequencing"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
)

// Message is the generic interface implemented by every sequencing message.
type Message interface {
	ProtoMessage() *seqproto.Message
}

type ProposedBlock struct {
	Block      *types.Block
	Commit     *types.ExtendedCommit
	PeerFilter *PeerRelayFilter
}

func ProposedBlockFromProto(pb *seqproto.ProposedBlock, filter *PeerRelayFilter) (*ProposedBlock, error) {
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
		Block:      block,
		Commit:     extCommit,
		PeerFilter: filter,
	}, nil
}

// ValidateBasic performs basic validation.
func (m *ProposedBlock) ValidateBasic() error {
	if m.Block == nil {
		return fmt.Errorf("nil block")
	}
	if err := m.Block.ValidateBasic(); err != nil {
		return fmt.Errorf("invalid block: %w", err)
	}
	if m.Commit == nil {
		return fmt.Errorf("nil commit")
	}
	if err := m.Commit.ValidateBasic(); err != nil {
		return fmt.Errorf("invalid commit: %w", err)
	}
	return nil
}

type AttestorCommit struct {
	Commit     *types.ExtendedCommit
	PeerFilter *PeerRelayFilter
}

func AttestorCommitFromProto(pb *seqproto.AttestorCommit, filter *PeerRelayFilter) (*AttestorCommit, error) {
	if pb == nil {
		return nil, fmt.Errorf("nil AttestorCommit proto")
	}
	commit, err := types.ExtendedCommitFromProto(pb.Commit)
	if err != nil {
		return nil, err
	}
	return &AttestorCommit{Commit: commit, PeerFilter: filter}, nil
}

func (m *AttestorCommit) ValidateBasic() error {
	if m.Commit == nil {
		return fmt.Errorf("nil commit")
	}
	if err := m.Commit.ValidateBasic(); err != nil {
		return fmt.Errorf("invalid commit: %w", err)
	}
	return nil
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
	PeerFilter     *PeerRelayFilter
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
	if m.PeerFilter != nil {
		br.PeerBloom = m.PeerFilter.MarshalBinary()
	}
	return &seqproto.Message{Sum: &seqproto.Message_BlockResponse{BlockResponse: br}}
}

func BlockResponseFromProto(pb *seqproto.BlockResponse) (*BlockResponse, error) {
	if pb == nil {
		return nil, fmt.Errorf("nil BlockResponse proto")
	}
	filter, err := PeerRelayFilterFromBytes(pb.PeerBloom)
	if err != nil {
		return nil, err
	}
	var proposedBlock *ProposedBlock
	if pb.ProposedBlock != nil {
		proposedBlock, err = ProposedBlockFromProto(pb.ProposedBlock, filter.Clone())
		if err != nil {
			return nil, err
		}
		if err := proposedBlock.ValidateBasic(); err != nil {
			return nil, err
		}
	}
	var attesterExtCommit *AttestorCommit
	if pb.AttesterCommit != nil {
		attesterExtCommit, err = AttestorCommitFromProto(pb.AttesterCommit, filter.Clone())
		if err != nil {
			return nil, err
		}
		if err := attesterExtCommit.ValidateBasic(); err != nil {
			return nil, err
		}
	}
	return &BlockResponse{ProposedBlock: proposedBlock, AttesterCommit: attesterExtCommit, PeerFilter: filter}, nil
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
