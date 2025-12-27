package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

const (
	RepoDir    = ".cndl"
	ObjectsDir = "objects"
	RefsDir    = "refs"
	LogDir     = "logs" // UNUSED
)

type Store struct {
	Root string
}

func New(projectRoot string) *Store {
	return &Store{
		Root: filepath.Join(projectRoot, RepoDir),
	}
}

func (s *Store) Init() error {
	paths := []string{
		filepath.Join(s.Root, ObjectsDir),
		filepath.Join(s.Root, RefsDir),
		filepath.Join(s.Root, LogDir),
	}

	for _, p := range paths {
		if err := os.MkdirAll(p, 0o0755); err != nil {
			return fmt.Errorf("failed to init store at %s: %w", p, err)
		}
	}
	return nil
}

func (s *Store) Exists() bool {
	info, err := os.Stat(s.Root)
	return err == nil && info.IsDir()
}

func (s *Store) Put(data []byte) (string, error) {
	hash := s.hash(data)
	shard := hash[:2]
	name := hash[2:]

	shardDir := filepath.Join(s.Root, ObjectsDir, shard)
	if err := os.MkdirAll(shardDir, 0o0755); err != nil {
		return "", fmt.Errorf("shard creation failed: %w", err)
	}

	path := filepath.Join(shardDir, name)

	// Idempotency check
	if _, err := os.Stat(path); err == nil {
		return hash, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return "", err
	}

	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("fsync failed: %w", err)
	}

	return hash, nil
}

func (s *Store) Get(hash string) ([]byte, error) {
	path := filepath.Join(s.Root, ObjectsDir, hash[:2], hash[2:])
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read object %s: %w", hash, err)
	}
	return data, nil
}

func (s *Store) Delete(hash string) error {
	path := filepath.Join(s.Root, ObjectsDir, hash[:2], hash[2:])
	return os.Remove(path)
}

func (s *Store) List() ([]string, error) {
	var hashes []string
	objRoot := filepath.Join(s.Root, ObjectsDir)

	err := filepath.Walk(objRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			shard := filepath.Base(filepath.Dir(path))
			hashes = append(hashes, shard+info.Name())
		}
		return nil
	})

	return hashes, err
}

func (s *Store) hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *Store) ResolvePath(prefix string) (string, error) {
	if len(prefix) < 3 {
		return "", fmt.Errorf("hash prefix too short")
	}

	shard := prefix[:2]
	dir := filepath.Join(s.Root, ObjectsDir, shard)

	files, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("object not found")
	}

	for _, f := range files {
		if len(f.Name()) >= len(prefix)-2 && f.Name()[:len(prefix)-2] == prefix[2:] {
			return filepath.Join(dir, f.Name()), nil
		}
	}

	return "", fmt.Errorf("no object matching prefix %s", prefix)
}
