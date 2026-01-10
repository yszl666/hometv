package main

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Cleaner manages storage space
type Cleaner struct {
	Limit int64 // Max size in bytes
	Store *Store
	mu    sync.Mutex
}

// NewCleaner creates a new cleaner
func NewCleaner(limit int64, store *Store) *Cleaner {
	return &Cleaner{
		Limit: limit,
		Store: store,
	}
}

// GetTotalSize returns the current total usage
func (c *Cleaner) GetTotalSize() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	var totalSize int64
	uploadDir := "uploads"

	// Calculate total size
	// Note: this might be slow if many files. 
	// Optimization: cache it or update it incrementally?
	// For now, simple walk.
	filepath.Walk(uploadDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	return totalSize
}

// CleanupOrphans removes directories in uploads that are not in the database
func (c *Cleaner) CleanupOrphans() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get all known video IDs
	c.Store.RLock()
	videos := c.Store.Videos
	c.Store.RUnlock()
	
	// Create a map for fast lookup
	validIDs := make(map[string]bool)
	for _, v := range videos {
		validIDs[v.ID] = true
	}
	
	uploadDir := "uploads"
	entries, err := os.ReadDir(uploadDir)
	if err != nil {
		log.Printf("Orphan cleaner error: %v", err)
		return
	}
	
	for _, entry := range entries {
		// We expect directories with ID name
		if !entry.IsDir() {
			continue
		}
		
		id := entry.Name()
		if !validIDs[id] {
			log.Printf("Found orphan directory (no DB record): %s. Deleting...", id)
			pathToDelete := filepath.Join(uploadDir, id)
			if err := os.RemoveAll(pathToDelete); err != nil {
				log.Printf("Failed to delete orphan %s: %v", id, err)
			}
		}
	}
}

// StartPeriodicCleanup starts a background routine to clean orphans
func (c *Cleaner) StartPeriodicCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			c.CleanupOrphans()
		}
	}()
}

// CheckAndClean checks total size and deletes old videos if limit exceeded
func (c *Cleaner) CheckAndClean() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var totalSize int64
	uploadDir := "uploads"

	// Calculate total size
	err := filepath.Walk(uploadDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		log.Printf("Cleaner error calculating size: %v", err)
		return
	}

	log.Printf("Storage usage: %.2f GB / %.2f GB", float64(totalSize)/1024/1024/1024, float64(c.Limit)/1024/1024/1024)

	if totalSize <= c.Limit {
		return
	}

	log.Println("Storage limit exceeded, cleaning up...")

	// Get videos sorted by time (oldest first)
	// We need to access store directly or use a method
	c.Store.RLock()
	videos := make([]Video, len(c.Store.Videos))
	copy(videos, c.Store.Videos)
	c.Store.RUnlock()

	// Sort by CreatedAt Ascending (Oldest first)
	sort.Slice(videos, func(i, j int) bool {
		return videos[i].CreatedAt.Before(videos[j].CreatedAt)
	})

	for _, v := range videos {
		if totalSize <= c.Limit {
			break
		}

		log.Printf("Deleting old video: %s", v.Title)

		// Delete files
		// Path is like /uploads/123/index.m3u8 or /uploads/filename.mp4
		// We need to delete the parent dir if it's HLS, or file if MP4
		
		var pathToDelete string
		if filepath.Base(v.Path) == "index.m3u8" {
			// It's HLS, delete directory: uploads/ID
			pathToDelete = filepath.Dir(strings.TrimPrefix(v.Path, "/"))
		} else {
			// It's MP4, delete file
			pathToDelete = strings.TrimPrefix(v.Path, "/")
		}

		// Calculate size of what we are deleting to update totalSize
		var deletedSize int64
		filepath.Walk(pathToDelete, func(_ string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				deletedSize += info.Size()
			}
			return nil
		})

		if err := os.RemoveAll(pathToDelete); err != nil {
			log.Printf("Error deleting %s: %v", pathToDelete, err)
			continue
		}

		// Remove from DB
		c.Store.DeleteVideo(v.ID)
		
		totalSize -= deletedSize
		log.Printf("Deleted %s, freed %.2f MB", v.Title, float64(deletedSize)/1024/1024)
	}
}

// Add DeleteVideo to Store
func (s *Store) DeleteVideo(id string) {
	s.Lock()
	// Remove defer s.Unlock() because we unlock manually before save
	// defer s.Unlock()
	
	for i, v := range s.Videos {
		if v.ID == id {
			s.Videos = append(s.Videos[:i], s.Videos[i+1:]...)
			break
		}
	}
	// Don't call saveSafe inside lock, it will deadlock!
	// Use saveInternal or unlock first.
	// But saveSafe locks.
	
	s.Unlock()
	// Now save
	s.saveSafe()
}
