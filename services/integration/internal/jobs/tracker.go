package jobs

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

type PipelineProgress struct {
	DocumentsProcessed int32
	DocumentsRemaining int32
	CurrentStage       string
}

type PipelineResult struct{}

type PipelineJob struct {
	ID            string
	IntegrationID string
	Status        string
	Progress      *PipelineProgress
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type JobTracker struct {
	mu   sync.RWMutex
	jobs map[string]*PipelineJob
}

func NewJobTracker() *JobTracker { return &JobTracker{jobs: map[string]*PipelineJob{}} }

func (t *JobTracker) Enqueue(ctx context.Context, job *PipelineJob) (*PipelineJob, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if job.ID == "" {
		job.ID = uuid.NewString()
	}
	job.Status = "PENDING"
	now := time.Now()
	job.CreatedAt = now
	job.UpdatedAt = now
	t.jobs[job.ID] = job
	return job, nil
}

func (t *JobTracker) Start(jobID string, run func(update func(ProgressUpdate))) {
	go func() {
		t.setStatus(jobID, "RUNNING")
		run(func(p ProgressUpdate) {
			if p.Final {
				t.setStatus(jobID, "COMPLETED")
			}
		})
	}()
}

type ProgressUpdate struct {
	Progress *PipelineProgress
	Final    bool
}

func (t *JobTracker) Get(ctx context.Context, jobID string) (*PipelineJob, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.jobs[jobID], nil
}

func (t *JobTracker) setStatus(jobID, status string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if j, ok := t.jobs[jobID]; ok {
		j.Status = status
		j.UpdatedAt = time.Now()
	}
}
