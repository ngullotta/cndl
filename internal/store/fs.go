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
)

func GetRepoPath() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("could not determine working directory: %w", err)
	}
	return filepath.Join(cwd, RepoDir), nil
}

func IsInitialized() bool {
	base, err := GetRepoPath()
	if err != nil {
		return false
	}
	info, err := os.Stat(base)
	return err == nil && info.IsDir()
}

func WriteObject(data []byte) (string, error) {
    hasher := sha256.New()
    hasher.Write(data)
    hash := hex.EncodeToString(hasher.Sum(nil))

    base, err := GetRepoPath()
    if err != nil {
        return "", err
    }

    shardDir := filepath.Join(base, ObjectsDir, hash[:2])
    
    if err := os.MkdirAll(shardDir, 0o0755); err != nil {
        return "", fmt.Errorf("failed to create shard dir: %w", err)
    }

    path := filepath.Join(shardDir, hash[2:])

    if _, err := os.Stat(path); err == nil {
        return hash, nil
    }

    fp, err := os.Create(path)
    if err != nil {
        return "", fmt.Errorf("failed to create object file: %w", err)
    }
    defer fp.Close()

    if _, err := fp.Write(data); err != nil {
        return "", fmt.Errorf("failed to write object: %w", err)
    }

    if err := fp.Sync(); err != nil {
        return "", err
    }

    return hash, nil
}

func ReadObject(hash string) ([]byte, error) {
	base, err := GetRepoPath()
	if err != nil {
		return nil, err
	}

	path := filepath.Join(base, ObjectsDir, hash[:2], hash[2:])
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read object %s: %w", hash, err)
	}
	return data, nil
}

func DeleteObject(hash string) error {
	base, err := GetRepoPath()
	if err != nil {
		return err
	}

	path := filepath.Join(base, ObjectsDir, hash[:2], hash[2:])
	return os.Remove(path)
}

func ListObjects() ([]string, error) {
	base, err := GetRepoPath()
	if err != nil {
		return nil, err
	}

	var hashes []string
	objRoot := filepath.Join(base, ObjectsDir)

	err = filepath.Walk(objRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			parentDir := filepath.Base(filepath.Dir(path))
			hashes = append(hashes, parentDir+info.Name())
		}
		return nil
	})

	return hashes, err
}

func InitRepository() error {
	paths := []string{
		filepath.Join(RepoDir, ObjectsDir),
		filepath.Join(RepoDir, "refs"),
	}

	for _, p := range paths {
		if err := os.MkdirAll(p, 0o0755); err != nil {
			return fmt.Errorf("failed to init repo at %s: %w", p, err)
		}
	}
	return nil
}