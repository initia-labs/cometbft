package types

import "errors"

var (
	ErrBatchDecodingError = errors.New("base64 decoding fail from single batch")
)
