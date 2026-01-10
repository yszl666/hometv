package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type TSFragment struct {
	Index int
	URL   string
	Path  string
	Key   []byte
	IV    []byte
}

// SetupDownload prepares the download: parses m3u8, creates directory, and generates local index.m3u8
// Returns the local m3u8 path, the list of fragments to download, and error
func SetupDownload(m3u8URL, videoID string) (string, []TSFragment, error) {
	// Setup client
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 30 * time.Second,
	}

	// 1. Get M3U8 content
	req, err := http.NewRequest("GET", m3u8URL, nil)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("failed to fetch m3u8: %v", err)
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read m3u8: %v", err)
	}

	lines := strings.Split(string(content), "\n")
	var fragments []TSFragment
	
	// Create final dir directly: uploads/<videoID>/
	finalDir := filepath.Join("uploads", videoID)
	if err := os.MkdirAll(finalDir, 0755); err != nil {
		return "", nil, err
	}

	// Generate local m3u8 file immediately
	localM3U8Path := filepath.Join(finalDir, "index.m3u8")
	localM3U8File, err := os.Create(localM3U8Path)
	if err != nil {
		return "", nil, err
	}
	
	// Encryption state
	var currentKey []byte
	var currentIV []byte
	
	// Parse lines and build fragments
	tsCount := 0
	
	// Write header to local m3u8
	localM3U8File.WriteString("#EXTM3U\n")
	localM3U8File.WriteString("#EXT-X-VERSION:3\n")
	localM3U8File.WriteString("#EXT-X-TARGETDURATION:10\n") // Estimate or update later
	localM3U8File.WriteString("#EXT-X-MEDIA-SEQUENCE:0\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			if strings.HasPrefix(line, "#EXT-X-KEY") {
				// Parse key: #EXT-X-KEY:METHOD=AES-128,URI="...",IV=...
				method := extractAttribute(line, "METHOD")
				if method != "AES-128" {
					return "", nil, fmt.Errorf("unsupported encryption method: %s", method)
				}
				
				keyURI := extractAttribute(line, "URI")
				keyURL := resolveURL(m3u8URL, keyURI)
				
				// Download key
				keyData, err := downloadBytes(context.Background(), client, keyURL)
				if err != nil {
					return "", nil, fmt.Errorf("failed to download key %s: %v", keyURL, err)
				}
				currentKey = keyData
				
				ivStr := extractAttribute(line, "IV")
				if ivStr != "" {
					ivStr = strings.TrimPrefix(ivStr, "0x")
					iv, err := hex.DecodeString(ivStr)
					if err != nil {
						return "", nil, fmt.Errorf("invalid IV: %v", err)
					}
					currentIV = iv
				} else {
					currentIV = nil // Will use sequence number
				}
				// Don't write KEY tag to local m3u8 as we decrypt content
			} else if strings.HasPrefix(line, "#EXTINF") {
				localM3U8File.WriteString(line + "\n")
			} else if strings.HasPrefix(line, "#EXT-X-ENDLIST") {
				localM3U8File.WriteString(line + "\n")
			}
			continue
		}
		
		// It's a URL
		fullURL := line
		if !strings.HasPrefix(line, "http") {
			fullURL = resolveURL(m3u8URL, line)
		}
		
		// Determine IV
		iv := currentIV
		if len(currentKey) > 0 && len(iv) == 0 {
			iv = make([]byte, 16)
			// TODO: Use correct sequence number logic if needed
		}

		tsFilename := fmt.Sprintf("%05d.ts", tsCount)
		fragments = append(fragments, TSFragment{
			Index: tsCount,
			URL:   fullURL,
			Path:  filepath.Join(finalDir, tsFilename),
			Key:   currentKey,
			IV:    iv,
		})
		
		// Add to local m3u8
		localM3U8File.WriteString(tsFilename + "\n")
		
		tsCount++
	}
	
	localM3U8File.Close()

	if len(fragments) == 0 {
		return "", nil, fmt.Errorf("no fragments found")
	}
	
	return "/uploads/" + videoID + "/index.m3u8", fragments, nil
}

// DownloadSegments downloads fragments concurrently
func DownloadSegments(ctx context.Context, fragments []TSFragment, onProgress func(downloaded, total int)) error {
	// Setup client
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 30 * time.Second,
	}

	// Use worker pool pattern to ensure sequential processing order
	// This prioritizes downloading earlier segments first for "play while downloading"
	numWorkers := 10
	taskChan := make(chan TSFragment, len(fragments))
	errChan := make(chan error, len(fragments))
	var wg sync.WaitGroup
	
	// Fill task channel in order
	for _, f := range fragments {
		taskChan <- f
	}
	close(taskChan)
	
	var downloadedCount int32
	totalFragments := int32(len(fragments))
	
	fmt.Printf("Downloading %d fragments...\n", totalFragments)

	// Report initial progress
	if onProgress != nil {
		onProgress(0, int(totalFragments))
	}

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range taskChan {
				// Check stop signal
				select {
				case <-ctx.Done():
					errChan <- fmt.Errorf("download stopped")
					return
				default:
				}

				// Check if already exists
				if _, err := os.Stat(f.Path); err == nil {
					newCount := atomic.AddInt32(&downloadedCount, 1)
					if onProgress != nil {
						onProgress(int(newCount), int(totalFragments))
					}
					continue
				}

				// Retry loop
				var lastErr error
				success := false
				for retry := 0; retry < 3; retry++ {
					// Use context for request to support cancellation
					data, err := downloadBytes(ctx, client, f.URL)
					if err != nil {
						// Check if cancelled
						if ctx.Err() != nil {
							errChan <- fmt.Errorf("download stopped")
							return
						}
						lastErr = err
						time.Sleep(time.Second)
						continue
					}
					
					// Decrypt if needed
					if len(f.Key) > 0 {
						data, err = decryptAES128(data, f.Key, f.IV)
						if err != nil {
							lastErr = fmt.Errorf("decrypt failed: %v", err)
							break // No retry for decrypt error
						}
					}
					
					if err := os.WriteFile(f.Path, data, 0644); err == nil {
						// Update progress
						newCount := atomic.AddInt32(&downloadedCount, 1)
						if onProgress != nil {
							onProgress(int(newCount), int(totalFragments))
						}
						// Print progress every 5 items or if it's the last one
						if newCount%5 == 0 || newCount == totalFragments {
							fmt.Printf("\rProgress: %.1f%% (%d/%d)", float64(newCount)/float64(totalFragments)*100, newCount, totalFragments)
						}
						success = true
						break
					} else {
						lastErr = err
						// Log retry
						fmt.Printf("\nRetry %d/3 for %s: %v\n", retry+1, f.URL, err)
					}
				}
				
				if !success {
					errChan <- fmt.Errorf("failed to download %s: %v", f.URL, lastErr)
				}
			}
		}()
	}

	wg.Wait()
	fmt.Println() // Newline after progress
	close(errChan)

	if len(errChan) > 0 {
		return <-errChan // Return first error
	}
	
	return nil
}

func downloadBytes(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func decryptAES128(data, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	if len(data)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("data length not multiple of block size")
	}

	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(data, data)

	// Remove PKCS#7 padding
	padding := int(data[len(data)-1])
	if padding > aes.BlockSize || padding == 0 {
		// Padding error, but sometimes stream might not be padded perfectly?
		// For TS streams, we might just ignore if it fails, but standard says PKCS7.
		// Let's be lenient or standard.
		return nil, fmt.Errorf("invalid padding")
	}
	
	for i := len(data) - padding; i < len(data); i++ {
		if data[i] != byte(padding) {
			return nil, fmt.Errorf("invalid padding byte")
		}
	}
	
	return data[:len(data)-padding], nil
}

func extractAttribute(line, attr string) string {
	// Simple regex extraction: attr="value" or attr=value
	// This is a naive implementation
	re := regexp.MustCompile(attr + `="([^"]+)"`)
	matches := re.FindStringSubmatch(line)
	if len(matches) > 1 {
		return matches[1]
	}
	re2 := regexp.MustCompile(attr + `=([^,]+)`)
	matches2 := re2.FindStringSubmatch(line)
	if len(matches2) > 1 {
		return matches2[1]
	}
	return ""
}

func resolveURL(base, relative string) string {
	u, err := url.Parse(relative)
	if err != nil {
		return relative
	}
	if u.IsAbs() {
		return relative
	}
	b, err := url.Parse(base)
	if err != nil {
		return relative
	}
	return b.ResolveReference(u).String()
}
