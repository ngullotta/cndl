package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	RepoDir    = ".cndl"
	ObjectsDir = "objects"
	RefsDir    = "refs"
)

type Commit struct {
	Parent    string            `json:"parent"`
	Timestamp int64             `json:"timestamp"`
	Message   string            `json:"message"`
	Snapshot  map[string]string `json:"snapshot"`
}

type Store struct {
	Root string
}

func New(projectRoot string) *Store {
	return &Store{Root: filepath.Join(projectRoot, RepoDir)}
}

func (s *Store) Init() error {
	for _, d := range []string{ObjectsDir, RefsDir} {
		if err := os.MkdirAll(filepath.Join(s.Root, d), 0o0755); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Put(data []byte) (string, error) {
	hash := s.hash(data)
	path := s.objectPath(hash)

	if _, err := os.Stat(path); err == nil {
		return hash, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o0755); err != nil {
		return "", err
	}

	return hash, os.WriteFile(path, data, 0o644)
}

func (s *Store) Get(hash string) ([]byte, error) {
	return os.ReadFile(s.objectPath(hash))
}

func (s *Store) Exists() bool {
	info, err := os.Stat(s.Root)
	return err == nil && info.IsDir()
}

func (s *Store) WriteRef(name string, hash string) error {
	path := filepath.Join(s.Root, RefsDir, strings.ToLower(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o0755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(hash), 0o0644)
}

func (s *Store) ReadRef(name string) (string, error) {
	data, err := os.ReadFile(filepath.Join(s.Root, RefsDir, strings.ToLower(name)))
	return string(data), err
}

func (s *Store) WriteCommit(c Commit) (string, error) {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "", err
	}
	return s.Put(data)
}

func (s *Store) ReadCommit(hash string) (Commit, error) {
	var c Commit
	data, err := s.Get(hash)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(data, &c)
	return c, err
}

func (s *Store) hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *Store) objectPath(hash string) string {
	return filepath.Join(s.Root, ObjectsDir, hash[:2], hash[2:])
}

func (s *Store) ResolvePath(prefix string) (string, error) {
	if len(prefix) < 3 {
		return "", fmt.Errorf("prefix too short")
	}
	dir := filepath.Join(s.Root, ObjectsDir, prefix[:2])
	files, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, f := range files {
		if strings.HasPrefix(f.Name(), prefix[2:]) {
			return filepath.Join(dir, f.Name()), nil
		}
	}
	return "", fmt.Errorf("object not found")
}
