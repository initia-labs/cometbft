package types

import (
	"time"
)

// Connect oracle query types for ABCI queries
// These are minimal gogoproto-compatible types to avoid importing the full skip-mev/connect module

// CurrencyPair represents a currency pair from the connect oracle module
type CurrencyPair struct {
	Base  string `protobuf:"bytes,1,opt,name=Base,proto3" json:"Base,omitempty"`
	Quote string `protobuf:"bytes,2,opt,name=Quote,proto3" json:"Quote,omitempty"`
}

func (m *CurrencyPair) Reset()         { *m = CurrencyPair{} }
func (m *CurrencyPair) String() string { return m.Base + "/" + m.Quote }
func (*CurrencyPair) ProtoMessage()    {}

// GetAllCurrencyPairsResponse is the response from GetAllCurrencyPairs query
type GetAllCurrencyPairsResponse struct {
	CurrencyPairs []CurrencyPair `protobuf:"bytes,1,rep,name=currency_pairs,json=currencyPairs,proto3" json:"currency_pairs"`
}

func (m *GetAllCurrencyPairsResponse) Reset()         { *m = GetAllCurrencyPairsResponse{} }
func (m *GetAllCurrencyPairsResponse) String() string { return "" }
func (*GetAllCurrencyPairsResponse) ProtoMessage()    {}

// GetPriceRequest is the request for GetPrice query
type GetPriceRequest struct {
	CurrencyPair string `protobuf:"bytes,1,opt,name=currency_pair,json=currencyPair,proto3" json:"currency_pair,omitempty"`
}

func (m *GetPriceRequest) Reset()         { *m = GetPriceRequest{} }
func (m *GetPriceRequest) String() string { return "" }
func (*GetPriceRequest) ProtoMessage()    {}

// QuotePrice represents the price data from connect oracle
type QuotePrice struct {
	Price          string    `protobuf:"bytes,1,opt,name=price,proto3" json:"price,omitempty"`
	BlockTimestamp time.Time `protobuf:"bytes,2,opt,name=block_timestamp,json=blockTimestamp,proto3,stdtime" json:"block_timestamp"`
	BlockHeight    uint64    `protobuf:"varint,3,opt,name=block_height,json=blockHeight,proto3" json:"block_height,omitempty"`
}

func (m *QuotePrice) Reset()         { *m = QuotePrice{} }
func (m *QuotePrice) String() string { return "" }
func (*QuotePrice) ProtoMessage()    {}

// GetPriceResponse is the response from GetPrice query
type GetPriceResponse struct {
	Price    *QuotePrice `protobuf:"bytes,1,opt,name=price,proto3" json:"price,omitempty"`
	Nonce    uint64      `protobuf:"varint,2,opt,name=nonce,proto3" json:"nonce,omitempty"`
	Decimals uint64      `protobuf:"varint,3,opt,name=decimals,proto3" json:"decimals,omitempty"`
	Id       uint64      `protobuf:"varint,4,opt,name=id,proto3" json:"id,omitempty"`
}

func (m *GetPriceResponse) Reset()         { *m = GetPriceResponse{} }
func (m *GetPriceResponse) String() string { return "" }
func (*GetPriceResponse) ProtoMessage()    {}
