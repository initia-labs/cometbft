package types

import (
	"testing"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/sequencing"
	coretypes "github.com/cometbft/cometbft/types"
)

func TestMsgRoundTripBlockResponse(t *testing.T) {
	lastCommit := &coretypes.Commit{
		Height: 0,
		Round:  0,
		Signatures: []coretypes.CommitSig{
			coretypes.NewCommitSigAbsent(),
		},
	}
	block := coretypes.MakeBlock(1, nil, lastCommit, nil)
	block.Header.ProposerAddress = make([]byte, 20)
	commit := &coretypes.ExtendedCommit{
		ExtendedSignatures: []coretypes.ExtendedCommitSig{
			coretypes.NewExtendedCommitSigAbsent(),
		},
	}

	filter := NewPeerRelayFilter()
	filter.Add("peerA")

	pb := &ProposedBlock{
		Block:      block,
		Commit:     commit.Clone(),
		PeerFilter: filter.Clone(),
	}
	ac := &AttestorCommit{
		Commit:     commit.Clone(),
		PeerFilter: filter.Clone(),
	}
	resp := &BlockResponse{
		ProposedBlock:  pb,
		AttesterCommit: ac,
		PeerFilter:     filter.Clone(),
	}

	proto := MsgToProto(resp)
	if proto == nil || proto.GetBlockResponse() == nil {
		t.Fatalf("expected block response proto, got %v", proto)
	}

	msg, err := MsgFromProto(proto)
	if err != nil {
		t.Fatalf("unexpected error decoding proto: %v", err)
	}
	decoded, ok := msg.(*BlockResponse)
	if !ok {
		t.Fatalf("decoded message has unexpected type %T", msg)
	}
	if decoded.ProposedBlock == nil || decoded.AttesterCommit == nil {
		t.Fatalf("expected proposed block and attestor commit unpacked")
	}
	if decoded.ProposedBlock.Block.Header.Height != block.Header.Height {
		t.Fatalf("expected block height %d, got %d", block.Header.Height, decoded.ProposedBlock.Block.Header.Height)
	}
	if decoded.AttesterCommit.Commit == nil {
		t.Fatalf("expected attestor commit present")
	}

	original := filter.MarshalBinary()
	if got := decoded.PeerFilter.MarshalBinary(); string(got) != string(original) {
		t.Fatalf("peer filter mismatch after round trip: %X vs %X", got, original)
	}

	decoded.ProposedBlock.PeerFilter.Add("new-peer")
	if decoded.PeerFilter.Contains("new-peer") {
		t.Fatalf("expected proposed block filter to be independent clone")
	}
	decoded.AttesterCommit.PeerFilter.Add("another-peer")
	if decoded.PeerFilter.Contains("another-peer") {
		t.Fatalf("expected attestor commit filter to be independent clone")
	}
}

func TestMsgRoundTripStatusUpdate(t *testing.T) {
	msg := &StatusUpdate{BaseHeight: 5, LastHeight: 17}
	proto := MsgToProto(msg)
	result, err := MsgFromProto(proto)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out, ok := result.(*StatusUpdate)
	if !ok {
		t.Fatalf("expected StatusUpdate, got %T", result)
	}
	if out.BaseHeight != msg.BaseHeight || out.LastHeight != msg.LastHeight {
		t.Fatalf("unexpected round-trip values: %#v vs %#v", out, msg)
	}
}

func TestMsgRoundTripBlockRequest(t *testing.T) {
	msg := &BlockRequest{Height: 42}
	proto := MsgToProto(msg)
	result, err := MsgFromProto(proto)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out, ok := result.(*BlockRequest)
	if !ok {
		t.Fatalf("expected BlockRequest, got %T", result)
	}
	if out.Height != msg.Height {
		t.Fatalf("expected height %d, got %d", msg.Height, out.Height)
	}
}

func TestMsgToProtoNil(t *testing.T) {
	if MsgToProto(nil) != nil {
		t.Fatalf("expected nil proto for nil message")
	}
}

func TestMsgFromProtoErrors(t *testing.T) {
	if _, err := MsgFromProto(nil); err == nil {
		t.Fatalf("expected error for nil proto message")
	}
	if _, err := MsgFromProto(&cmtproto.Message{}); err == nil {
		t.Fatalf("expected error for message with unknown payload")
	}
}

func TestProposedBlockFromProtoNil(t *testing.T) {
	if _, err := ProposedBlockFromProto(nil, nil); err == nil {
		t.Fatalf("expected error for nil proposed block")
	}
}

func TestAttestorCommitFromProtoNil(t *testing.T) {
	if _, err := AttestorCommitFromProto(nil, nil); err == nil {
		t.Fatalf("expected error for nil attestor commit")
	}
}

func TestBlockResponseFromProtoInvalidBloom(t *testing.T) {
	proto := &cmtproto.BlockResponse{
		PeerBloom: []byte{0x01}, // too short to contain header + payload
	}
	if _, err := BlockResponseFromProto(proto); err == nil {
		t.Fatalf("expected error decoding invalid bloom filter payload")
	}
}

func TestBlockResponseFromProtoFiltersCloned(t *testing.T) {
	filter := NewPeerRelayFilter()
	filter.Add("peerZ")
	proto := &cmtproto.BlockResponse{
		PeerBloom: filter.MarshalBinary(),
	}

	resp, err := BlockResponseFromProto(proto)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.PeerFilter == nil {
		t.Fatalf("expected peer filter restored")
	}
	resp.PeerFilter.Add("new-peer")
	if filter.Contains("new-peer") {
		t.Fatalf("expected original filter to remain unchanged")
	}
}
