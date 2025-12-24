package main

import (
	"cndl/internal/store"
	"cndl/internal/utils"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"hash/crc32"
	"log"
)

var (
	ErrInvalidChecksum = errors.New("checksum mismatch: data is corrupted")
	ErrTooSmall        = errors.New("file too small to be a valid chunk")
)

func WrapChunk(c chunkenc.Chunk) []byte {
	raw := c.Bytes()

	res := make([]byte, 1+len(raw)+4)

	res[0] = byte(c.Encoding())
	copy(res[1:], raw)

	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(res[:1+len(raw)], table)

	binary.BigEndian.PutUint32(res[1+len(raw):], checksum)

	return res
}

func ReadAndValidateChunk(data []byte) (chunkenc.Chunk, error) {
	if len(data) < 5 {
		return nil, ErrTooSmall
	}

	payload := data[:len(data)-4]
	want := binary.BigEndian.Uint32(data[len(data)-4:])

	table := crc32.MakeTable(crc32.Castagnoli)
	got := crc32.Checksum(payload, table)

	if got != want {
		return nil, ErrInvalidChecksum
	}

	encByte := payload[0]
	encoding := chunkenc.Encoding(encByte)

	if chunkenc.Encoding(encByte) != chunkenc.EncXOR {
		return nil, fmt.Errorf("unsupported encoding type: %d", encoding)
	}

	c := chunkenc.NewXORChunk()
	c.Reset(payload[1:])

	return c, nil
}

func main() {
	c := chunkenc.NewXORChunk()
	appender, _ := c.Appender()
	for i, v := range utils.GenerateGBM(100, 7200, 0, 0.001) {
		appender.Append(int64(i)+1, v)
	}

	data := WrapChunk(c)
	hash, err := store.WriteObject(data)
	if err != nil {
		log.Fatalf("Error saving data: %v", err)
	}
	fmt.Printf("Successfully committed object: %s\n", hash)

	newChunk, err := ReadAndValidateChunk(data)
	if err != nil {
		fmt.Printf("Failed to read object: %v\n", err)
		return
	}

	it := newChunk.Iterator(nil)

	for {
		if it.Next() == chunkenc.ValNone {
			break
		}

		t, v := it.At()
		fmt.Printf("Timestamp: %d, Value: %f\n", t, v)
	}

	if err := it.Err(); err != nil {
		fmt.Printf("Error during iteration: %v\n", err)
	}
}
