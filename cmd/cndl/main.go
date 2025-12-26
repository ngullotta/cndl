package main

import (
	"fmt"
	"os"
	"hash/crc32"
	"encoding/binary"

	"cndl/internal/utils"
	"cndl/internal/store"
	"github.com/spf13/cobra"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
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

func main() {
	var root = &cobra.Command{Use: "cndl"}

	var initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize the data store",
		Run: func(cmd *cobra.Command, args []string) {
			err := store.InitRepository()
			if err != nil {
				fmt.Printf("Failed to init: %v\n", err)
				os.Exit(1)
			}
			path, _ := store.GetRepoPath()
			fmt.Printf("Initialized Cndl repo at: %s\n", path)
		},
	}

	var addCmd = &cobra.Command{
		Use:   "add [ticker]",
		Short: "Generate and add a data chunk for a ticker",
		Args:  cobra.ExactArgs(1), // Requires exactly 1 argument (the ticker)
		Run: func(cmd *cobra.Command, args []string) {
			ticker := args[0]

			if !store.IsInitialized() {
				fmt.Println("Error: Not a cndl repository. Run 'cndl init' first.")
				os.Exit(1)
			}

			c := chunkenc.NewXORChunk()
			appender, _ := c.Appender()
			for i, v := range utils.GenerateGBM(100, 7200, 0, 0.001) {
				appender.Append(int64(i), v)
			}

			data := WrapChunk(c)

			hash, err := store.WriteObject(data)
			if err != nil {
				fmt.Printf("Failed to store: %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("Ticker: %s\n", ticker)
			fmt.Printf("Stored as Object: %s\n", hash)
		},
	}

	root.AddCommand(initCmd, addCmd)
	root.Execute()
}