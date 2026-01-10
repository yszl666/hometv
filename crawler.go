package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
) 

// Crawler manages the crawling process
type Crawler struct {
	Client *http.Client
}

// NewCrawler creates a new crawler
func NewCrawler() *Crawler {
	// Skip TLS verify to avoid some SSL errors
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	return &Crawler{
		Client: &http.Client{
			Transport: tr,
			Timeout:   30 * time.Second,
		},
	}
}

// Episode represents a single episode
type Episode struct {
	Title string
	URL   string
}

// Crawl starts the crawling process for a given TV show URL
func (c *Crawler) Crawl(showURL string, startEpisodeOverride int) error {
	log.Printf("Starting crawl for: %s", showURL)

	// 1. Fetch episodes list
	seriesTitle, episodes, err := c.fetchEpisodes(showURL)
	if err != nil {
		return fmt.Errorf("failed to fetch episodes: %v", err)
	}

	log.Printf("Found %d episodes for series: %s", len(episodes), seriesTitle)

	// Save series URL for future refreshing
	if seriesStore != nil {
		seriesStore.AddSeries(seriesTitle, showURL)
		// If user provided a specific start episode, save it
		if startEpisodeOverride >= 1 {
			seriesStore.SetStartEpisode(seriesTitle, startEpisodeOverride)
		}
	}

	// Check start episode setting
	startEpisode := 1
	if startEpisodeOverride >= 1 {
		startEpisode = startEpisodeOverride
	} else if seriesStore != nil {
		if info, ok := seriesStore.GetSeriesInfo(seriesTitle); ok {
			startEpisode = info.StartEpisode
			if startEpisode == 0 { startEpisode = 1 }
		}
	}

	// Check if we should skip existing series
	// We want to avoid adding tasks for episodes that already exist or are in progress.
	// But simply checking series name isn't enough if new episodes are added.
	// However, user said "duplicate series" - maybe they mean adding the same full series again.
	
	// Let's count how many episodes of this series we already have
	store.RLock()
	existingCount := 0
	for _, v := range store.Videos {
		if v.Series == seriesTitle {
			existingCount++
		}
	}
	store.RUnlock()
	
	if existingCount >= len(episodes) {
		log.Printf("Series %s already exists with %d/%d episodes. Skipping...", seriesTitle, existingCount, len(episodes))
		return nil
	}

	// 2. Process each episode
	// Limit concurrency or just do it sequentially to be safe
	
	for _, ep := range episodes {
		// Check if this specific episode already exists
		store.RLock()
		exists := false
		for _, v := range store.Videos {
			if v.Series == seriesTitle && v.Title == ep.Title {
				exists = true
				break
			}
		}
		store.RUnlock()
		
		if exists {
			log.Printf("Skipping existing episode: %s", ep.Title)
			continue
		}

		log.Printf("Processing episode: %s", ep.Title)
		
		// Check start episode
		// Try to extract episode number from title
		epNum := extractEpisodeNumber(ep.Title)
		if epNum > 0 && epNum < startEpisode {
			log.Printf("Skipping episode %s (Episode %d < StartEpisode %d)", ep.Title, epNum, startEpisode)
			continue
		}
		
		// Check if already exists (simple check by filename)
		// We'll use a sanitized title for filename
		filename := fmt.Sprintf("%d_%s.mp4", time.Now().Unix(), sanitizeFilename(ep.Title))
		// Actually, let's use the title directly if possible but unique
		// To avoid duplicates, maybe we should check if a video with same title exists in store?
		// For now, let's just download.
		
		m3u8URL, err := c.fetchM3U8(ep.URL)
		if err != nil {
			log.Printf("Error fetching M3U8 for %s: %v", ep.Title, err)
			continue
		}
		
		if m3u8URL == "" {
			log.Printf("No M3U8 found for %s", ep.Title)
			continue
		}

		log.Printf("Found M3U8 for %s: %s", ep.Title, m3u8URL)

		// 3. Download
		err = c.downloadVideo(m3u8URL, filename, ep.Title, seriesTitle)
		if err != nil {
			log.Printf("Error downloading %s: %v", ep.Title, err)
			continue
		}
		
		log.Printf("Successfully downloaded: %s", ep.Title)
	}

	return nil
}

func (c *Crawler) fetchEpisodes(showURL string) (string, []Episode, error) {
	req, err := http.NewRequest("GET", showURL, nil)
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := c.Client.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", nil, fmt.Errorf("status code %d", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", nil, err
	}

	var episodes []Episode
	baseURL, _ := url.Parse(showURL)
	
	// Get Series Title
	seriesTitle := strings.TrimSpace(doc.Find("h1").First().Text())
	if seriesTitle == "" {
		seriesTitle = strings.TrimSpace(doc.Find("title").First().Text())
	}
	// Clean up common suffixes if needed, e.g. " - 在线播放"
	if idx := strings.Index(seriesTitle, " -"); idx != -1 {
		seriesTitle = seriesTitle[:idx]
	}

	// Selector based on observation: .module-play-list-link
	// Try to find the first playlist container to avoid duplicates from multiple sources
	playlist := doc.Find(".module-play-list").First()
	selection := playlist.Find("a.module-play-list-link")
	
	// Fallback if no container found or empty
	if selection.Length() == 0 {
		selection = doc.Find("a.module-play-list-link")
	}

	selection.Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists {
			return
		}
		epTitle := strings.TrimSpace(s.Text())
		
		// Combine titles
		fullTitle := fmt.Sprintf("%s %s", seriesTitle, epTitle)
		
		// Handle relative URLs
		fullURL := href
		if strings.HasPrefix(href, "/") {
			fullURL = fmt.Sprintf("%s://%s%s", baseURL.Scheme, baseURL.Host, href)
		}

		episodes = append(episodes, Episode{
			Title: fullTitle,
			URL:   fullURL,
		})
	})

	return seriesTitle, episodes, nil
}

func (c *Crawler) fetchM3U8(episodeURL string) (string, error) {
	req, err := http.NewRequest("GET", episodeURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := c.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	htmlContent := string(bodyBytes)

	// Regex to find "url":"https://...m3u8"
	// The content we saw was: "url":"https:\/\/m3u8.vhmzy.com\/videos\/...\/index.m3u8"
	// Be careful with escaped slashes
	// Update: Use non-greedy match [^"]+ to stop at the next quote
	re := regexp.MustCompile(`"url"\s*:\s*"(https?:\\/\\/[^"]+\.m3u8)"`)
	matches := re.FindStringSubmatch(htmlContent)
	
	if len(matches) > 1 {
		// Unescape using strconv.Unquote to handle \/ and \uXXXX
		// We wrap it in quotes to make it a valid string literal for Unquote
		unquoted, err := strconv.Unquote(`"` + matches[1] + `"`)
		if err != nil {
			// Fallback to simple replace if unquote fails
			return strings.ReplaceAll(matches[1], `\/`, `/`), nil
		}
		return unquoted, nil
	}

	return "", nil
}

func (c *Crawler) downloadVideo(m3u8URL, filename, title, series string) error {
	// Generate video ID first to use as directory name
	videoID := fmt.Sprintf("%d", time.Now().UnixNano())
	
	video := Video{
		ID:        videoID,
		Title:     title,
		Series:    series,
		Path:      "/uploads/" + videoID + "/index.m3u8",
		SourceURL: m3u8URL,
		Status:    "pending",
		CreatedAt: time.Now(),
		Size:      0, 
	}

	if err := store.AddVideo(video); err != nil {
		return err
	}
	
	// Add to TaskManager
	if taskManager != nil {
		taskManager.Add(DownloadTask{
			VideoID:   videoID,
			SourceURL: m3u8URL,
			Title:     title,
		})
	}
	
	return nil
}

func sanitizeFilename(name string) string {
	// Replace invalid characters
	invalid := []string{"/", "\\", ":", "*", "?", "\"", "<", ">", "|"}
	for _, char := range invalid {
		name = strings.ReplaceAll(name, char, "_")
	}
	return strings.TrimSpace(name)
}

func extractEpisodeNumber(title string) int {
	// Common patterns: 
	// "第01集"
	// "第1集"
	// "EP01"
	// "01" (if standalone or at end)
	// "SeriesName 01"
	
	// Try "第(\d+)集"
	re1 := regexp.MustCompile(`第(\d+)集`)
	matches := re1.FindStringSubmatch(title)
	if len(matches) > 1 {
		num, _ := strconv.Atoi(matches[1])
		return num
	}
	
	// Try "EP(\d+)" case insensitive
	re2 := regexp.MustCompile(`(?i)EP(\d+)`)
	matches2 := re2.FindStringSubmatch(title)
	if len(matches2) > 1 {
		num, _ := strconv.Atoi(matches2[1])
		return num
	}
	
	// Try just number at the end
	re3 := regexp.MustCompile(`\s(\d+)$`)
	matches3 := re3.FindStringSubmatch(title)
	if len(matches3) > 1 {
		num, _ := strconv.Atoi(matches3[1])
		return num
	}

	return 0
}
