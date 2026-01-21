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

// GetPricesRequest is the request for a batch GetPrices query
type GetPricesRequest struct {
	CurrencyPairIds []string `protobuf:"bytes,1,rep,name=currency_pair_ids,json=currencyPairIds,proto3" json:"currency_pair_ids,omitempty"`
}

func (m *GetPricesRequest) Reset()         { *m = GetPricesRequest{} }
func (m *GetPricesRequest) String() string { return "" }
func (*GetPricesRequest) ProtoMessage()    {}

// QuotePrice represents the price data from connect oracle
type QuotePrice struct {
	Price          string    `protobuf:"bytes,1,opt,name=price,proto3" json:"price,omitempty"`
	BlockTimestamp time.Time `protobuf:"bytes,2,opt,name=block_timestamp,json=blockTimestamp,proto3,stdtime" json:"block_timestamp"`
	BlockHeight    uint64    `protobuf:"varint,3,opt,name=block_height,json=blockHeight,proto3" json:"block_height,omitempty"`
}

func (m *QuotePrice) Reset()         { *m = QuotePrice{} }
func (m *QuotePrice) String() string { return "" }
func (*QuotePrice) ProtoMessage()    {}

// GetPriceResponse is the response for a single price
type GetPriceResponse struct {
	Price    *QuotePrice `protobuf:"bytes,1,opt,name=price,proto3" json:"price,omitempty"`
	Nonce    uint64      `protobuf:"varint,2,opt,name=nonce,proto3" json:"nonce,omitempty"`
	Decimals uint64      `protobuf:"varint,3,opt,name=decimals,proto3" json:"decimals,omitempty"`
	Id       uint64      `protobuf:"varint,4,opt,name=id,proto3" json:"id,omitempty"`
}

func (m *GetPriceResponse) Reset()         { *m = GetPriceResponse{} }
func (m *GetPriceResponse) String() string { return "" }
func (*GetPriceResponse) ProtoMessage()    {}

// GetPricesResponse is the response from batch GetPrices query
type GetPricesResponse struct {
	Prices []GetPriceResponse `protobuf:"bytes,1,rep,name=prices,proto3" json:"prices"`
}

func (m *GetPricesResponse) Reset()         { *m = GetPricesResponse{} }
func (m *GetPricesResponse) String() string { return "" }
func (*GetPricesResponse) ProtoMessage()    {}
