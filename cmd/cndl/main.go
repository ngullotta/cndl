package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"

	"cndl/internal/store"
	"cndl/internal/utils"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/spf13/cobra"
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
	cwd, _ := os.Getwd()
	s := store.New(cwd)

	var rootCmd = &cobra.Command{
		Use:   "cndl",
		Short: "TBA",
	}

	var initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize the data store",
		Run: func(cmd *cobra.Command, args []string) {
			if err := s.Init(); err != nil {
				fmt.Printf("Failed to init: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Initialized Cndl repo at: %s\n", s.Root)
		},
	}

	var addCmd = &cobra.Command{
		Use:   "add [ticker]",
		Short: "Generate and add a data chunk for a ticker",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if !s.Exists() {
				fmt.Println("Error: Not a cndl repository. Run 'cndl init' first.")
				os.Exit(1)
			}

			c := chunkenc.NewXORChunk()
			appender, _ := c.Appender()

			prices := utils.GenerateGBM(100, 7200, 0, 0.001)
			for i, v := range prices {
				appender.Append(int64(i), v)
			}

			data := WrapChunk(c)

			hash, err := s.Put(data)
			if err != nil {
				fmt.Printf("Failed to store: %v\n", err)
				os.Exit(1)
			}

			symbol := args[0]
			if err := s.WriteRef("fetch/"+symbol, hash); err != nil {
				fmt.Printf("failed to update ref: %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("Object Hash: %s\n", hash)
		},
	}

	var showCmd = &cobra.Command{
		Use:   "show [hash]",
		Short: "Examine the contents of a data chunk",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if !s.Exists() {
				fmt.Println("Error: Not a cndl repository.")
				os.Exit(1)
			}

			path, err := s.ResolvePath(args[0])
			if err != nil {
				fmt.Printf("%v\n", err)
				os.Exit(1)
			}

			data, _ := os.ReadFile(path)

			payloadLen := len(data) - 4
			want := binary.BigEndian.Uint32(data[payloadLen:])
			got := crc32.Checksum(data[:payloadLen], crc32.MakeTable(crc32.Castagnoli))

			if want != got {
				fmt.Println("Error: Checksum mismatch! Data is corrupted.")
				os.Exit(1)
			}

			chunk, err := chunkenc.FromData(chunkenc.Encoding(data[0]), data[1:payloadLen])
			if err != nil {
				fmt.Printf("Failed to parse chunk: %v\n", err)
				os.Exit(1)
			}

			it := chunk.Iterator(nil)
			var firstT, lastT int64
			var firstV, lastV float64
			count := 0

			for it.Next() != chunkenc.ValNone {
				t, v := it.At()
				if count == 0 {
					firstT, firstV = t, v
				}
				lastT, lastV = t, v
				count++
			}

			fmt.Printf("Object: %s\n", args[0])
			fmt.Printf("Samples: %d\n", count)
			fmt.Printf("Start:  T=%d | Price: %.2f\n", firstT, firstV)
			fmt.Printf("End:    T=%d | Price: %.2f\n", lastT, lastV)
			fmt.Printf("Change: %.2f%%\n", ((lastV-firstV)/firstV)*100)
			fmt.Printf("Checksum: 0x%X\n", (got))
		},
	}

	var commitCmd = &cobra.Command{
		Use:   "commit -m [message]",
		Short: "Record a snapshot of the staged data",
		Run: func(cmd *cobra.Command, args []string) {
			msg, _ := cmd.Flags().GetString("message")

			parent, _ := s.ReadRef("heads/main")

			newSnapshot := make(map[string]string)

			if parent != "" {
				oldCommit, _ := s.ReadCommit(parent)
				for k, v := range oldCommit.Snapshot {
					newSnapshot[k] = v
				}
			}

			symbols, _ := filepath.Glob(filepath.Join(s.Root, "refs/fetch/*"))
			for _, path := range symbols {
				ticker := filepath.Base(path)
				hash, _ := os.ReadFile(path)
				newSnapshot[ticker] = string(hash)
			}

			c := store.Commit{
				Parent:    parent,
				Timestamp: time.Now().Unix(),
				Message:   msg,
				Snapshot:  newSnapshot,
			}

			commitHash, err := s.WriteCommit(c)
			if err != nil {
				fmt.Printf("Failed to commit: %v\n", err)
				return
			}

			s.WriteRef("heads/main", commitHash)

			fmt.Printf("Created commit: %s\n", commitHash[:8])
			fmt.Printf("Message: %s\n", msg)
		},
	}

	rootCmd.AddCommand(initCmd, addCmd, showCmd, commitCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
