// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package bloombits

import (
	"testing"
)

func TestBloom(t *testing.T) {
	positive := []string{
		"testtest",
		"test",
		"hallo",
		"other",
	}
	negative := []string{
		"tes",
		"lo",
	}

	var bloom Bloom
	for _, data := range positive {
		bloom.Add([]byte(data))
	}

	for _, data := range positive {
		if !bloom.Test([]byte(data)) {
			t.Error("expected", data, "to test true")
		}
	}
	for _, data := range negative {
		if bloom.Test([]byte(data)) {
			t.Error("did not expect", data, "to test true")
		}
	}
}

func BenchmarkBloom9(b *testing.B) {
	test := []byte("testestestest")
	for i := 0; i < b.N; i++ {
		Bloom9(test)
	}
}

func BenchmarkBloom9Lookup(b *testing.B) {
	toTest := []byte("testtest")
	bloom := new(Bloom)
	for i := 0; i < b.N; i++ {
		bloom.Test(toTest)
	}
}
