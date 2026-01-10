package main

import (
	"context"
	"log"
	"sync"
)

// DownloadTask represents a download job
type DownloadTask struct {
	VideoID   string
	SourceURL string
	Title     string
}

// TaskManager manages download tasks
type TaskManager struct {
	TaskQueue     chan DownloadTask
	PriorityQueue chan DownloadTask
	RunningTasks  sync.Map // map[string]context.CancelFunc
	CurrentTaskID string
	mu            sync.Mutex
}

var taskManager *TaskManager

// NewTaskManager creates a new task manager
func NewTaskManager() *TaskManager {
	return &TaskManager{
		TaskQueue:     make(chan DownloadTask, 1000), // Buffer for pending tasks
		PriorityQueue: make(chan DownloadTask, 1000), // High priority buffer
	}
}

// Start begins processing tasks
func (tm *TaskManager) Start() {
	go func() {
		for {
			var task DownloadTask
			select {
			case task = <-tm.PriorityQueue:
				// Got priority task
			default:
				// No priority task, check normal queue
				select {
				case task = <-tm.PriorityQueue:
					// Double check priority
				case task = <-tm.TaskQueue:
					// Got normal task
				}
			}
			
			log.Printf("Processing task: %s (%s)", task.Title, task.VideoID)
			tm.processTask(task)
		}
	}()
}

// Add adds a task to the queue
func (tm *TaskManager) Add(task DownloadTask) {
	tm.TaskQueue <- task
}

// ResumeDownloads finds unfinished tasks and adds them to the queue
func (tm *TaskManager) ResumeDownloads(store *Store) {
	store.RLock()
	// Collect tasks first to avoid potential deadlock if channel blocks and processTask needs Lock
	var tasksToResume []DownloadTask
	for _, v := range store.Videos {
		if v.Status == "pending" || v.Status == "downloading" {
			tasksToResume = append(tasksToResume, DownloadTask{
				VideoID:   v.ID,
				SourceURL: v.SourceURL,
				Title:     v.Title,
			})
		}
	}
	store.RUnlock()

	count := 0
	for _, task := range tasksToResume {
		log.Printf("Resuming download for: %s", task.Title)
		tm.Add(task)
		count++
	}
	log.Printf("Resumed %d tasks", count)
}

// CancelTask cancels a running task
func (tm *TaskManager) CancelTask(videoID string) {
	if cancel, ok := tm.RunningTasks.Load(videoID); ok {
		log.Printf("Cancelling task: %s", videoID)
		cancel.(context.CancelFunc)()
		tm.RunningTasks.Delete(videoID)
	}
}

// RetryWithPreemption adds a task to priority queue and preempts current task
func (tm *TaskManager) RetryWithPreemption(retryTask DownloadTask) {
	tm.mu.Lock()
	currentID := tm.CurrentTaskID
	tm.mu.Unlock()

	// 1. Queue the retry task first (so it runs immediately after current stops)
	tm.PriorityQueue <- retryTask

	// 2. If something is running, interrupt it
	if currentID != "" {
		// Check if it's the same task (don't interrupt itself)
		if currentID == retryTask.VideoID {
			return
		}

		if cancel, ok := tm.RunningTasks.Load(currentID); ok {
			log.Printf("Preempting task %s for retry of %s", currentID, retryTask.Title)
			
			// Get current task info to re-queue
			if video, found := store.GetVideo(currentID); found {
				// Re-queue current task to priority queue (after retry task)
				tm.PriorityQueue <- DownloadTask{
					VideoID:   video.ID,
					SourceURL: video.SourceURL,
					Title:     video.Title,
				}
				
				// Reset status to pending so it doesn't stay as "downloading"
				store.UpdateVideoStatus(currentID, "pending")
			}
			
			// Cancel execution
			cancel.(context.CancelFunc)()
		}
	}
}

func (tm *TaskManager) processTask(task DownloadTask) {
	// Add retry logic to run after task completes (success or failure)
	defer tm.AutoRetryAllPending(task.VideoID)

	// Update CurrentTaskID
	tm.mu.Lock()
	tm.CurrentTaskID = task.VideoID
	tm.mu.Unlock()
	
	defer func() {
		tm.mu.Lock()
		if tm.CurrentTaskID == task.VideoID {
			tm.CurrentTaskID = ""
		}
		tm.mu.Unlock()
	}()

	// Check if video still exists (wasn't deleted while in queue)
	_, exists := store.GetVideo(task.VideoID)
	if !exists {
		log.Printf("Task skipped (video deleted): %s", task.Title)
		return
	}

	// Setup context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	tm.RunningTasks.Store(task.VideoID, cancel)
	defer func() {
		cancel()
		tm.RunningTasks.Delete(task.VideoID)
	}()

	// Update status to downloading
	if err := store.UpdateVideoStatus(task.VideoID, "downloading"); err != nil {
		log.Printf("Task stopped (video deleted?): %s", task.Title)
		return
	}

	// 1. Prepare download (parse m3u8, create local file, get fragments)
	// Even if it was partially downloaded, SetupDownload will regenerate the list
	// and DownloadSegments will skip existing files.
	// NOTE: SetupDownload overwrites index.m3u8. This is fine.
	_, fragments, err := SetupDownload(task.SourceURL, task.VideoID)
	if err != nil {
		// Check if preempted/cancelled
		if ctx.Err() != nil {
			log.Printf("Task preempted/cancelled during setup: %s", task.Title)
			return
		}
		log.Printf("Setup failed for %s: %v", task.Title, err)
		store.UpdateVideoStatus(task.VideoID, "error")
		return
	}

	// 2. Download segments
	if err := DownloadSegments(ctx, fragments, func(downloaded, total int) {
		store.UpdateVideoProgress(task.VideoID, downloaded, total)
	}); err != nil {
		// Check if preempted/cancelled
		if ctx.Err() != nil {
			log.Printf("Task preempted/cancelled during download: %s", task.Title)
			return
		}
		log.Printf("Download failed (or stopped) for %s: %v", task.Title, err)
		store.UpdateVideoStatus(task.VideoID, "error")
		return
	}

	// 3. Complete
	if err := store.UpdateVideoStatus(task.VideoID, "completed"); err != nil {
		log.Printf("Failed to mark completed (video deleted?): %s", task.Title)
		return
	}
	log.Printf("Completed download for: %s", task.Title)
	
	// Trigger cleaner
	if cleaner != nil {
		go cleaner.CheckAndClean()
	}
}

// AutoRetryAllPending finds all uncompleted tasks and queues them
func (tm *TaskManager) AutoRetryAllPending(finishedTaskID string) {
	// Strategy:
	// Find ALL tasks that are (error OR pending) and NOT current task
	// Sort them by ID (time)
	// Add them to queue if they are likely not in queue (hard to know, but safe to re-add)
	
	// Actually, if we just add them all, we might duplicate.
	// But the user wants "try one by one from the first".
	// If we add them to the queue, they will be processed in order.
	
	// To avoid flooding the queue with duplicates every time a task finishes:
	// We should probably only add ONE task at a time? 
	// The user said "after completing a task, sequentially retry ALL preceding unfinished tasks".
	// This implies a continuous process.
	
	// If we just add the *next* oldest task, that creates a chain reaction.
	// When that one finishes, it calls AutoRetryAllPending again, adding the next one.
	// This seems like the right approach to avoid queue flooding.
	// Just pick the oldest one.
	
	// But wait, what if there are multiple workers? (Currently 1)
	// If we pick the oldest, and it's already in queue but far back, re-adding it moves it to back?
	// No, adding to channel puts it at back.
	// If we want to prioritize "preceding" tasks (older tasks), we should maybe use PriorityQueue?
	// But priority queue is for user actions.
	
	// If we use normal queue, they go to back.
	// If we want to ensure older tasks run first, we should perhaps re-queue them to PriorityQueue?
	// Or just rely on the fact that we pick the *oldest* one.
	
	// Let's stick to the previous logic: Pick the single oldest target video.
	// This creates a chain: Task A finishes -> Pick Oldest (B) -> Add B.
	// Task B finishes -> Pick Oldest (C) -> Add C.
	// This effectively "retries all preceding tasks sequentially".
	
	tm.AutoRetryFirstError(finishedTaskID)
}

// AutoRetryFirstError finds the oldest unfinished task and queues it
func (tm *TaskManager) AutoRetryFirstError(skipID string) {
	// Find the video with smallest ID that is (error OR pending)
	store.RLock()
	var targetVideo *Video
	
	for _, v := range store.Videos {
		if v.Status == "error" || v.Status == "pending" {
			// Skip if it's the current running task
			if v.ID == tm.CurrentTaskID {
				continue
			}
			// Skip if it is the task that just finished (to avoid immediate loop on failure)
			if v.ID == skipID {
				continue
			}
			
			if targetVideo == nil {
				val := v
				targetVideo = &val
			} else {
				// Compare IDs (smaller is older/priority)
				if v.ID < targetVideo.ID {
					val := v
					targetVideo = &val
				}
			}
		}
	}
	store.RUnlock()

	if targetVideo != nil {
		log.Printf("Auto-scheduling oldest task: %s [%s]", targetVideo.Title, targetVideo.Status)
		
		// If it was error, reset to pending
		if targetVideo.Status == "error" {
			store.UpdateVideoStatus(targetVideo.ID, "pending")
		}

		// Add to normal queue
		tm.Add(DownloadTask{
			VideoID:   targetVideo.ID,
			SourceURL: targetVideo.SourceURL,
			Title:     targetVideo.Title,
		})
	}
}
