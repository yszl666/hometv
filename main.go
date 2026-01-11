package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Video represents a video file metadata
type Video struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Series    string    `json:"series"` // New field for series name
	Path      string    `json:"path"`
	SourceURL string    `json:"source_url"`
	Status    string    `json:"status"` // pending, downloading, completed, error
	CreatedAt time.Time `json:"created_at"`
	Size      int64     `json:"size"`
	TotalSegments      int             `json:"total_segments"`
	DownloadedSegments int             `json:"downloaded_segments"`
	WatchedSegments    map[string]bool `json:"watched_segments,omitempty"`
	IsWatched          bool            `json:"is_watched"`
}

// SeriesInfo holds series configuration
type SeriesInfo struct {
	URL          string `json:"url"`
	StartEpisode int    `json:"start_episode"` // 0 or 1 means start from beginning
}

// SeriesStore manages series metadata
type SeriesStore struct {
	sync.RWMutex
	Series   map[string]SeriesInfo // Name -> Info
	FilePath string
}

func NewSeriesStore(filePath string) *SeriesStore {
	s := &SeriesStore{
		Series:   make(map[string]SeriesInfo),
		FilePath: filePath,
	}
	s.load()
	return s
}

func (s *SeriesStore) load() {
	s.Lock()
	defer s.Unlock()
	file, err := os.Open(s.FilePath)
	if err != nil {
		return
	}
	defer file.Close()
	
	// Handle migration from map[string]string to map[string]SeriesInfo
	var raw map[string]interface{}
	if err := json.NewDecoder(file).Decode(&raw); err != nil {
		return
	}

	s.Series = make(map[string]SeriesInfo)
	for k, v := range raw {
		switch val := v.(type) {
		case string:
			// Old format: value is URL string
			s.Series[k] = SeriesInfo{URL: val, StartEpisode: 1}
		case map[string]interface{}:
			// New format: value is object
			url, _ := val["url"].(string)
			startEp, _ := val["start_episode"].(float64)
			s.Series[k] = SeriesInfo{URL: url, StartEpisode: int(startEp)}
		}
	}
}

func (s *SeriesStore) save() error {
	s.Lock()
	defer s.Unlock()
	file, err := os.Create(s.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(s.Series)
}

func (s *SeriesStore) AddSeries(name, url string) {
	s.Lock()
	// Preserve existing StartEpisode if updating URL
	if info, ok := s.Series[name]; ok {
		if info.URL != url {
			info.URL = url
			s.Series[name] = info
			s.Unlock()
			s.save()
		} else {
			s.Unlock()
		}
		return
	}
	
	s.Series[name] = SeriesInfo{URL: url, StartEpisode: 1}
	s.Unlock()
	s.save()
}

func (s *SeriesStore) SetStartEpisode(name string, episode int) {
	s.Lock()
	if info, ok := s.Series[name]; ok {
		info.StartEpisode = episode
		s.Series[name] = info
	} else {
		// Should not happen usually, but if setting before crawling?
		// We need URL to be useful. Assume series exists.
	}
	s.Unlock()
	s.save()
}

func (s *SeriesStore) GetSeriesInfo(name string) (SeriesInfo, bool) {
	s.RLock()
	defer s.RUnlock()
	info, ok := s.Series[name]
	return info, ok
}

func (s *SeriesStore) GetSeriesURL(name string) string {
	s.RLock()
	defer s.RUnlock()
	if info, ok := s.Series[name]; ok {
		return info.URL
	}
	return ""
}

func (s *SeriesStore) DeleteSeries(name string) {
	s.Lock()
	delete(s.Series, name)
	s.Unlock()
	s.save()
}

// ProgressMessage for SSE
type ProgressMessage struct {
	VideoID    string `json:"video_id"`
	Downloaded int    `json:"downloaded"`
	Total      int    `json:"total"`
	Status     string `json:"status"`
}

// Broker manages SSE clients
type Broker struct {
	clients    map[chan ProgressMessage]bool
	register   chan chan ProgressMessage
	unregister chan chan ProgressMessage
	broadcast  chan ProgressMessage
	lock       sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		clients:    make(map[chan ProgressMessage]bool),
		register:   make(chan chan ProgressMessage),
		unregister: make(chan chan ProgressMessage),
		broadcast:  make(chan ProgressMessage),
	}
}

func (b *Broker) Run() {
	for {
		select {
		case client := <-b.register:
			b.lock.Lock()
			b.clients[client] = true
			b.lock.Unlock()
		case client := <-b.unregister:
			b.lock.Lock()
			if _, ok := b.clients[client]; ok {
				delete(b.clients, client)
				close(client)
			}
			b.lock.Unlock()
		case msg := <-b.broadcast:
			b.lock.Lock()
			for client := range b.clients {
				select {
				case client <- msg:
				default:
					// Skip if blocked
				}
			}
			b.lock.Unlock()
		}
	}
}

var progressBroker *Broker

// Store manages video data
type Store struct {
	sync.RWMutex
	Videos   []Video
	FilePath string
}

// NewStore creates a new video store
func NewStore(filePath string) *Store {
	// Ensure directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("Error creating data directory: %v", err)
	}

	s := &Store{
		Videos:   make([]Video, 0),
		FilePath: filePath,
	}
	s.load()
	return s
}

// load reads data from disk
func (s *Store) load() {
	s.Lock()
	defer s.Unlock()

	file, err := os.Open(s.FilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("Error opening data file: %v", err)
		return
	}
	defer file.Close()

	if err := json.NewDecoder(file).Decode(&s.Videos); err != nil {
		log.Printf("Error decoding video data: %v", err)
	}
}

// save writes data to disk
func (s *Store) save() error {
	s.Lock()
	defer s.Unlock()

	file, err := os.Create(s.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewEncoder(file).Encode(s.Videos)
}

// AddVideo adds a video and saves to disk
func (s *Store) AddVideo(v Video) error {
	s.Lock()
	s.Videos = append(s.Videos, v)
	s.Unlock()
	return s.saveSafe()
}

// UpdateVideoStatus updates the status of a video
func (s *Store) UpdateVideoStatus(id, status string) error {
	s.Lock()
	found := false
	for i := range s.Videos {
		if s.Videos[i].ID == id {
			s.Videos[i].Status = status
			found = true
			break
		}
	}
	s.Unlock()
	
	if found {
		return s.saveSafe()
	}
	return fmt.Errorf("video not found")
}

// UpdateVideoProgress updates the progress of a video
func (s *Store) UpdateVideoProgress(id string, downloaded, total int) error {
	s.Lock()
	found := false
	var status string
	for i := range s.Videos {
		if s.Videos[i].ID == id {
			s.Videos[i].DownloadedSegments = downloaded
			s.Videos[i].TotalSegments = total
			status = s.Videos[i].Status
			found = true
			break
		}
	}
	s.Unlock()
	
	if found {
		// Broadcast progress
		if progressBroker != nil {
			progressBroker.broadcast <- ProgressMessage{
				VideoID:    id,
				Downloaded: downloaded,
				Total:      total,
				Status:     status,
			}
		}
		// We don't save to disk on every progress update to avoid IO thrashing
		// Only save periodically or on status change
		return nil
	}
	return fmt.Errorf("video not found")
}

func (s *Store) saveSafe() error {
	s.Lock()
	defer s.Unlock()
	return s.saveNoLock()
}

func (s *Store) saveNoLock() error {
	// Re-sort before saving to ensure consistency, though we sort on read usually.
	// Let's keep it simple.
	
	file, err := os.Create(s.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewEncoder(file).Encode(s.Videos)
}

// MarkSegmentWatched marks a segment as watched and checks for completion
func (s *Store) MarkSegmentWatched(videoID, segmentName string) error {
	s.Lock()
	defer s.Unlock()

	var video *Video
	for i := range s.Videos {
		if s.Videos[i].ID == videoID {
			video = &s.Videos[i]
			break
		}
	}

	if video == nil {
		return fmt.Errorf("video not found")
	}

	if video.IsWatched {
		return nil
	}

	if video.WatchedSegments == nil {
		video.WatchedSegments = make(map[string]bool)
	}

	video.WatchedSegments[segmentName] = true

	// Check if we should mark as watched (95% threshold)
	// Only if we have total segments info
	if video.TotalSegments > 0 {
		watchedCount := len(video.WatchedSegments)
		threshold := int(float64(video.TotalSegments) * 0.95)
		
		if watchedCount >= threshold {
			log.Printf("Video %s (%s) reached 95%% watched (%d/%d). Marking as watched and cleaning up.", 
				video.Title, video.ID, watchedCount, video.TotalSegments)
			
			video.IsWatched = true
			video.Status = "watched" // Update status to indicate it's done and files are gone
			
			// Trigger cleanup in background with delay
			// User might still be watching the end, so wait 5 minutes
			go func(vid, vpath string) {
				log.Printf("Scheduling cleanup for %s in 5 minutes...", vid)
				time.Sleep(5 * time.Minute)
				s.cleanupVideoFiles(vid, vpath)
			}(video.ID, video.Path)
			
			// Save immediately
			return s.saveNoLock()
		} else if watchedCount%10 == 0 {
			// Save progress periodically
			return s.saveNoLock()
		}
	}

	return nil
}

// cleanupVideoFiles deletes TS files but keeps the M3U8 and metadata
func (s *Store) cleanupVideoFiles(videoID, videoPath string) {
	log.Printf("Cleaning up TS files for video %s", videoID)
	
	// Determine directory
	var dir string
	if strings.HasSuffix(videoPath, "index.m3u8") {
		// /uploads/ID/index.m3u8
		// dir is uploads/ID (relative) or /uploads/ID (absolute-ish)
		// Our paths in DB are /uploads/...
		// We serve from "uploads" dir.
		
		// Strip leading slash
		relPath := strings.TrimPrefix(videoPath, "/")
		dir = filepath.Dir(relPath)
	} else {
		// Not M3U8? Maybe MP4.
		// If MP4, we just delete the file?
		// User said "delete corresponding ts files". 
		// If it's MP4, maybe we shouldn't delete it?
		// But "viewed" logic applies to segments. MP4 doesn't have segments in this context usually.
		return
	}
	
	// Verify it is inside uploads
	if !strings.HasPrefix(dir, "uploads/") {
		log.Printf("Unsafe cleanup path: %s", dir)
		return
	}
	
	// List files in directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("Failed to read dir %s: %v", dir, err)
		return
	}
	
	for _, entry := range entries {
		name := entry.Name()
		// Delete .ts files
		if strings.HasSuffix(name, ".ts") {
			err := os.Remove(filepath.Join(dir, name))
			if err != nil {
				log.Printf("Failed to remove %s: %v", name, err)
			}
		}
	}
	log.Printf("Cleanup completed for %s", videoID)
}

// GetVideos returns all videos sorted by time desc
func (s *Store) GetVideos() []Video {
	s.RLock()
	defer s.RUnlock()
	
	// Create a copy to sort
	videos := make([]Video, len(s.Videos))
	copy(videos, s.Videos)
	
	sort.Slice(videos, func(i, j int) bool {
		return videos[i].CreatedAt.After(videos[j].CreatedAt)
	})
	
	return videos
}

// GetVideo returns a video by ID
func (s *Store) GetVideo(id string) (Video, bool) {
	s.RLock()
	defer s.RUnlock()
	for _, v := range s.Videos {
		if v.ID == id {
			return v, true
		}
	}
	return Video{}, false
}

var store *Store
var seriesStore *SeriesStore
var cleaner *Cleaner

func main() {
	// Initialize store
	dataPath := filepath.Join("data", "videos.json")
	store = NewStore(dataPath)
	
	// Initialize SeriesStore
	seriesPath := filepath.Join("data", "series.json")
	seriesStore = NewSeriesStore(seriesPath)
	
	// Initialize SSE Broker
	progressBroker = NewBroker()
	go progressBroker.Run()
	
	// Initialize TaskManager
	taskManager = NewTaskManager()
	taskManager.Start()
	taskManager.ResumeDownloads(store)
	
	// Initialize cleaner (30GB = 30 * 1024 * 1024 * 1024)
	cleaner = NewCleaner(30*1024*1024*1024, store)
	
	// Run cleaner on startup
	go cleaner.CheckAndClean()
	
	// Start periodic orphan cleanup (1 hour)
	go cleaner.CleanupOrphans() // Run immediately on startup
	cleaner.StartPeriodicCleanup(1 * time.Hour)

	// Static files
	// Wrap uploads handler to track watched segments
	uploadsHandler := http.StripPrefix("/uploads/", http.FileServer(http.Dir("uploads")))
	http.Handle("/uploads/", watchedHandler(uploadsHandler))
	
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	// Routes
	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/play", playHandler)
	http.HandleFunc("/upload", uploadPageHandler)
	http.HandleFunc("/crawler", crawlerPageHandler)
	http.HandleFunc("/api/upload", uploadAPIHandler)
	http.HandleFunc("/api/crawl", crawlAPIHandler)
	http.HandleFunc("/api/delete", deleteAPIHandler)
	http.HandleFunc("/api/delete_series", deleteSeriesAPIHandler)
	http.HandleFunc("/api/progress", progressAPIHandler)
	http.HandleFunc("/api/refresh", refreshAPIHandler)
	http.HandleFunc("/api/retry", retryAPIHandler)
	http.HandleFunc("/api/update_series_settings", updateSeriesSettingsAPIHandler)

	log.Println("Server starting on :8080...")
	log.Println("Visit http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	videos := store.GetVideos()
	
	// Check if viewing a specific series
	seriesName := r.URL.Query().Get("series")
	
	if seriesName != "" {
		// Filter by series
		var seriesVideos []Video
		for _, v := range videos {
			if v.Series == seriesName {
				seriesVideos = append(seriesVideos, v)
			}
		}
		
		// Sort by Title usually for episodes? Or CreatedAt?
		// Usually episodes are added in order, so CreatedAt is fine, but maybe reverse?
		// Let's keep store order (which is desc time).
		// But for episodes, we often want Ascending.
		// Let's reverse for series view so Episode 1 is first.
		sort.Slice(seriesVideos, func(i, j int) bool {
			return seriesVideos[i].CreatedAt.Before(seriesVideos[j].CreatedAt)
		})

		funcMap := template.FuncMap{
			"episodeNum": extractEpisodeNumber,
		}

		tmpl, err := template.New("index.html").Funcs(funcMap).ParseFiles("templates/index.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		
		data := struct {
			Videos []Video
			Series string
		}{
			Videos: seriesVideos,
			Series: seriesName,
		}
		
		tmpl.Execute(w, data)
		return
	}

	// Group by Series
	seriesMap := make(map[string][]Video)
	for _, v := range videos {
		s := v.Series
		if s == "" {
			s = "其他"
		}
		seriesMap[s] = append(seriesMap[s], v)
	}

	// Create Series List
	type SeriesInfo struct {
		Name      string
		Count     int
		CoverPath string // Use first video
		UpdatedAt time.Time
	}
	
	var seriesList []SeriesInfo
	for name, vList := range seriesMap {
		// vList is sorted by time desc (from GetVideos)
		// So first item is latest
		seriesList = append(seriesList, SeriesInfo{
			Name:      name,
			Count:     len(vList),
			CoverPath: vList[0].Path, // Note: video tag uses Path, but for cover maybe we need a poster? For now use video.
			UpdatedAt: vList[0].CreatedAt,
		})
	}
	
	// Sort series by updated time
	sort.Slice(seriesList, func(i, j int) bool {
		return seriesList[i].UpdatedAt.After(seriesList[j].UpdatedAt)
	})
	
	// Calculate global total size
	var globalTotalSize int64
	if cleaner != nil {
		globalTotalSize = cleaner.GetTotalSize()
	}

	// Add StartEpisode info to SeriesList for template
	// We need to define a new struct that includes StartEpisode or extend SeriesInfo
	type SeriesInfoWithSettings struct {
		SeriesInfo // embed
		StartEpisode int
	}
	
	var seriesListWithSettings []SeriesInfoWithSettings
	for _, s := range seriesList {
		startEp := 1
		if info, ok := seriesStore.GetSeriesInfo(s.Name); ok {
			startEp = info.StartEpisode
		}
		if startEp == 0 { startEp = 1 }
		
		seriesListWithSettings = append(seriesListWithSettings, SeriesInfoWithSettings{
			SeriesInfo: s,
			StartEpisode: startEp,
		})
	}
	
	tmpl, err := template.ParseFiles("templates/series_list.html")
	if err != nil {
		// Fallback if template doesn't exist (yet)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	data := struct {
		Series []SeriesInfoWithSettings
		TotalSize string
	}{
		Series: seriesListWithSettings,
		TotalSize: formatSize(globalTotalSize),
	}
	
	tmpl.Execute(w, data)
}

func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func playHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	video, found := store.GetVideo(id)
	if !found {
		http.NotFound(w, r)
		return
	}

	// Register template functions
	funcMap := template.FuncMap{
		"sub": func(a, b int) int {
			return a - b
		},
		"hasSuffix": strings.HasSuffix,
	}

	tmpl, err := template.New("play.html").Funcs(funcMap).ParseFiles("templates/play.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	tmpl.Execute(w, video)
}

func uploadPageHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFiles("templates/upload.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl.Execute(w, nil)
}

func uploadAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 100MB limit
	r.ParseMultipartForm(100 << 20)

	file, handler, err := r.FormFile("video")
	if err != nil {
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Ensure filename is unique
	filename := fmt.Sprintf("%d_%s", time.Now().Unix(), handler.Filename)
	filePath := filepath.Join("uploads", filename)

	// Create destination file
	dst, err := os.Create(filePath)
	if err != nil {
		http.Error(w, "Error saving file", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	// Copy uploaded content
	if _, err := io.Copy(dst, file); err != nil {
		http.Error(w, "Error saving file", http.StatusInternalServerError)
		return
	}

	// Save metadata
	video := Video{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Title:     handler.Filename,
		Path:      "/uploads/" + filename,
		CreatedAt: time.Now(),
		Size:      handler.Size,
	}

	if err := store.AddVideo(video); err != nil {
		http.Error(w, "Error saving metadata", http.StatusInternalServerError)
		return
	}
	
	// Trigger cleaner
	go cleaner.CheckAndClean()

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func crawlerPageHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFiles("templates/crawler.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl.Execute(w, nil)
}

func crawlAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	targetURL := r.FormValue("url")
	if targetURL == "" {
		http.Error(w, "URL is required", http.StatusBadRequest)
		return
	}

	startEpStr := r.FormValue("start_episode")
	startEp := 0
	if startEpStr != "" {
		fmt.Sscanf(startEpStr, "%d", &startEp)
	}

	// Start crawling in a goroutine
	go func() {
		crawler := NewCrawler()
		if err := crawler.Crawl(targetURL, startEp); err != nil {
			log.Printf("Crawler error: %v", err)
		}
	}()

	// Show a message to the user
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `
		<!DOCTYPE html>
		<html>
		<head>
			<meta http-equiv="refresh" content="3;url=/">
			<style>body{background:#121212;color:white;text-align:center;padding-top:50px;font-family:sans-serif;}</style>
		</head>
		<body>
			<h1>任务已提交</h1>
			<p>爬虫正在后台运行，视频将陆续出现在首页...</p>
			<p>3秒后返回首页</p>
		</body>
		</html>
	`)
}

func deleteAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.FormValue("id")
	if id == "" {
		http.Error(w, "ID required", http.StatusBadRequest)
		return
	}

	video, found := store.GetVideo(id)
	if !found {
		http.NotFound(w, r)
		return
	}

	// Delete files
	var pathToDelete string
	if filepath.Base(video.Path) == "index.m3u8" {
		// It's HLS, delete directory: uploads/ID
		// video.Path is like /uploads/ID/index.m3u8
		// we want uploads/ID
		pathToDelete = filepath.Dir(strings.TrimPrefix(video.Path, "/"))
	} else {
		// It's MP4, delete file
		pathToDelete = strings.TrimPrefix(video.Path, "/")
	}
	
	// Safety check: ensure we are deleting inside uploads to avoid accidents
	if strings.HasPrefix(pathToDelete, "uploads/") {
		 log.Printf("Deleting files: %s", pathToDelete)
		 os.RemoveAll(pathToDelete)
	}

	// Cancel task if running
	if taskManager != nil {
		taskManager.CancelTask(id)
	}

	// Delete from DB
	store.DeleteVideo(id)
	
	w.WriteHeader(http.StatusOK)
}

func watchedHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		// Check if it's a TS file request inside uploads
		// Expected path format: /uploads/{videoID}/{segment}.ts
		if strings.HasPrefix(path, "/uploads/") && strings.HasSuffix(path, ".ts") {
			parts := strings.Split(path, "/")
			// ["", "uploads", "videoID", "segment.ts"]
			if len(parts) >= 4 {
				videoID := parts[2]
				segmentName := parts[3]
				
				// Mark as watched in background
				go func() {
					if err := store.MarkSegmentWatched(videoID, segmentName); err != nil {
						// Ignore errors
					}
				}()
			}
		}
		next.ServeHTTP(w, r)
	})
}

func refreshAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	seriesStore.RLock()
	// Copy map to avoid holding lock during crawl
	seriesToRefresh := make(map[string]string)
	
	targetSeries := r.FormValue("series")
	if targetSeries != "" {
		if info, ok := seriesStore.Series[targetSeries]; ok {
			seriesToRefresh[targetSeries] = info.URL
		}
	} else {
		for name, info := range seriesStore.Series {
			seriesToRefresh[name] = info.URL
		}
	}
	seriesStore.RUnlock()
	
	count := len(seriesToRefresh)
	if count == 0 {
		if targetSeries != "" {
			fmt.Fprintf(w, "Series not found or no source URL")
		} else {
			fmt.Fprintf(w, "No series to refresh")
		}
		return
	}

	go func() {
		log.Printf("Starting refresh for %d series...", count)
		crawler := NewCrawler()
		for name, url := range seriesToRefresh {
			log.Printf("Refreshing series: %s", name)
			// Pass 0 to use saved settings
			if err := crawler.Crawl(url, 0); err != nil {
				log.Printf("Failed to refresh series %s: %v", name, err)
			}
			// Add a small delay between series to be polite
			if count > 1 {
				time.Sleep(2 * time.Second)
			}
		}
		log.Println("Refresh completed")
	}()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"message": "Started refreshing %d series"}`, count)
}

func retryAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.FormValue("id")
	if id == "" {
		http.Error(w, "ID required", http.StatusBadRequest)
		return
	}

	video, found := store.GetVideo(id)
	if !found {
		http.NotFound(w, r)
		return
	}

	// Reset status
	if err := store.UpdateVideoStatus(id, "pending"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Add to task manager with preemption
	if taskManager != nil {
		taskManager.RetryWithPreemption(DownloadTask{
			VideoID:   video.ID,
			SourceURL: video.SourceURL,
			Title:     video.Title,
		})
	}

	w.WriteHeader(http.StatusOK)
}

func updateSeriesSettingsAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	seriesName := r.FormValue("series")
	startEpStr := r.FormValue("start_episode")
	
	if seriesName == "" || startEpStr == "" {
		http.Error(w, "Missing series or start_episode", http.StatusBadRequest)
		return
	}
	
	startEp := 1
	fmt.Sscanf(startEpStr, "%d", &startEp)
	if startEp < 1 { startEp = 1 }
	
	seriesStore.SetStartEpisode(seriesName, startEp)
	
	w.WriteHeader(http.StatusOK)
}

func progressAPIHandler(w http.ResponseWriter, r *http.Request) {
	// Set headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Register client
	messageChan := make(chan ProgressMessage)
	progressBroker.register <- messageChan

	// Unregister when connection closes
	defer func() {
		progressBroker.unregister <- messageChan
	}()

	// Send initial status of all active downloads?
	// Or just wait for updates.
	// Maybe better to send initial state.
	// But client will load page with initial state from HTML.
	// So only updates are needed.

	// Listen for messages
	for {
		select {
		case msg := <-messageChan:
			// Format as SSE
			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

func deleteSeriesAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	seriesName := r.FormValue("series")
	if seriesName == "" {
		http.Error(w, "Series name required", http.StatusBadRequest)
		return
	}

	// Find all videos in series
	videos := store.GetVideos()
	var videosToDelete []Video
	for _, v := range videos {
		if v.Series == seriesName {
			videosToDelete = append(videosToDelete, v)
		}
	}
	
	if len(videosToDelete) == 0 {
		http.NotFound(w, r)
		return
	}
	
	log.Printf("Deleting series: %s (%d videos)", seriesName, len(videosToDelete))

	for _, video := range videosToDelete {
		// Delete files
		var pathToDelete string
		if filepath.Base(video.Path) == "index.m3u8" {
			pathToDelete = filepath.Dir(strings.TrimPrefix(video.Path, "/"))
		} else {
			pathToDelete = strings.TrimPrefix(video.Path, "/")
		}
		
		if strings.HasPrefix(pathToDelete, "uploads/") {
			 os.RemoveAll(pathToDelete)
		}

		// Cancel task if running
		if taskManager != nil {
			taskManager.CancelTask(video.ID)
		}

		// Delete from DB
		store.DeleteVideo(video.ID)
	}
	
	// Also delete from series store
	seriesStore.DeleteSeries(seriesName)
	
	w.WriteHeader(http.StatusOK)
}
