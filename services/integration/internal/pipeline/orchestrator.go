package pipeline

import (
	"context"
	"fmt"
	"time"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	integrationv1 "github.com/redbco/redb-open/api/proto/integration/v1"
	"github.com/redbco/redb-open/services/integration/internal/pipeline/steps"
	"github.com/redbco/redb-open/services/integration/internal/transform"
)

// PipelineConfig holds orchestrator configuration
type PipelineConfig struct {
	AnchorAddress string
	DefaultBatch  int
	WorkerCount   int
}

// PipelineMetrics is a placeholder for metrics collection hooks
type PipelineMetrics struct{}

// RAGPipelineOrchestrator coordinates the RAG pipeline across steps
type RAGPipelineOrchestrator struct {
	anchorClient anchorv1.AnchorServiceClient
	ragManager   RAGManagerInterface
	jobTracker   JobTrackerInterface
	config       *PipelineConfig
	metrics      *PipelineMetrics
}

// Interfaces to decouple packages
type (
	// RAGManagerInterface is implemented by rag.RAGProviderManager
	RAGManagerInterface interface {
		GetProvider(ctx context.Context, integrationID string) (steps.RAGProvider, error)
	}

	// JobTrackerInterface is implemented by jobs.JobTracker
	JobTrackerInterface interface {
		Enqueue(ctx context.Context, job *PipelineJob) (*PipelineJob, error)
		Start(jobID string, run func(update func(PipelineProgressUpdate)))
		Get(ctx context.Context, jobID string) (*PipelineJob, error)
	}
)

// PipelineJob mirrors the jobs package minimal fields for orchestration
type PipelineJob struct {
	ID            string
	IntegrationID string
	Status        string
	Progress      *PipelineProgress
	Configuration *integrationv1.ExecuteRAGPipelineRequest
	Result        *integrationv1.RAGPipelineResult
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// PipelineProgress basic tracking
type PipelineProgress struct {
	DocumentsProcessed int32
	DocumentsRemaining int32
	CurrentStage       string
}

// PipelineProgressUpdate used for streaming updates
type PipelineProgressUpdate struct {
	Progress *PipelineProgress
	Final    *integrationv1.RAGPipelineResult
}

// NewOrchestrator constructs a RAGPipelineOrchestrator
func NewOrchestrator(anchorClient anchorv1.AnchorServiceClient, ragManager RAGManagerInterface, jobTracker JobTrackerInterface, cfg *PipelineConfig) *RAGPipelineOrchestrator {
	return &RAGPipelineOrchestrator{
		anchorClient: anchorClient,
		ragManager:   ragManager,
		jobTracker:   jobTracker,
		config:       cfg,
		metrics:      &PipelineMetrics{},
	}
}

// ExecutePipeline runs the pipeline synchronously
func (o *RAGPipelineOrchestrator) ExecutePipeline(ctx context.Context, req *integrationv1.ExecuteRAGPipelineRequest) (*integrationv1.RAGPipelineResult, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	// Step 1: extract
	extractor := &steps.SourceExtractor{
		AnchorClient: o.anchorClient,
		BatchSize:    int(req.Source.GetBatchSize()),
		Transformer:  transform.NewDataTransformer(),
	}
	if extractor.BatchSize <= 0 {
		if o.config != nil && o.config.DefaultBatch > 0 {
			extractor.BatchSize = o.config.DefaultBatch
		} else {
			extractor.BatchSize = 100
		}
	}

	docBatches, err := extractor.Extract(ctx, req.Source)
	if err != nil {
		return nil, err
	}

	// Step 2: process via RAG
	processor := &steps.RAGProcessor{RAGManager: o.ragManager, Workers: max(1, getWorkers(o))}
	resultsCh, err := processor.Process(ctx, docBatches, req.IntegrationId)
	if err != nil {
		return nil, err
	}

	// Step 3: store results
	storage := &steps.TargetStorage{AnchorClient: o.anchorClient, BatchSize: extractor.BatchSize}
	storageResult, err := storage.Store(ctx, resultsCh, req.Target)
	if err != nil {
		return nil, err
	}

	// Collate summary
	return &integrationv1.RAGPipelineResult{
		ProcessedDocuments: storageResult.ProcessedDocuments,
		StoredEmbeddings:   storageResult.StoredEmbeddings,
		FailedDocuments:    storageResult.FailedDocuments,
	}, nil
}

// ExecutePipelineAsync runs the pipeline asynchronously with job tracking
func (o *RAGPipelineOrchestrator) ExecutePipelineAsync(ctx context.Context, req *integrationv1.ExecuteRAGPipelineRequest) (string, error) {
	if o.jobTracker == nil {
		return "", fmt.Errorf("job tracker not configured")
	}
	job := &PipelineJob{
		ID:            fmt.Sprintf("job_%d", time.Now().UnixNano()),
		IntegrationID: req.IntegrationId,
		Status:        "PENDING",
		Configuration: req,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	if _, err := o.jobTracker.Enqueue(ctx, job); err != nil {
		return "", err
	}
	o.jobTracker.Start(job.ID, func(update func(PipelineProgressUpdate)) {
		// Execute synchronously inside the runner
		res, err := o.ExecutePipeline(context.Background(), req)
		if err != nil {
			update(PipelineProgressUpdate{Final: &integrationv1.RAGPipelineResult{Errors: []string{err.Error()}}})
			return
		}
		update(PipelineProgressUpdate{Final: res})
	})
	return job.ID, nil
}

// ExecutePipelineStream runs pipeline and emits progress updates
func (o *RAGPipelineOrchestrator) ExecutePipelineStream(ctx context.Context, req *integrationv1.ExecuteRAGPipelineStreamRequest) (<-chan PipelineProgressUpdate, error) {
	updates := make(chan PipelineProgressUpdate, 8)
	go func() {
		defer close(updates)
		// Build equivalent non-stream request
		execReq := &integrationv1.ExecuteRAGPipelineRequest{
			IntegrationId: req.IntegrationId,
			Source:        req.Source,
			Processing:    req.Processing,
			Target:        req.Target,
		}
		res, err := o.ExecutePipeline(ctx, execReq)
		if err != nil {
			updates <- PipelineProgressUpdate{Final: &integrationv1.RAGPipelineResult{Errors: []string{err.Error()}}}
			return
		}
		updates <- PipelineProgressUpdate{Final: res}
	}()
	return updates, nil
}

// ResumePipeline would resume a previously failed job
func (o *RAGPipelineOrchestrator) ResumePipeline(ctx context.Context, jobID string) error {
	if o.jobTracker == nil {
		return fmt.Errorf("job tracker not configured")
	}
	// Minimal stub; real implementation would reload job state and continue from last checkpoint
	_, err := o.jobTracker.Get(ctx, jobID)
	if err != nil {
		return err
	}
	return fmt.Errorf("resume not implemented yet")
}

func getWorkers(o *RAGPipelineOrchestrator) int {
	if o == nil || o.config == nil || o.config.WorkerCount <= 0 {
		return 4
	}
	return o.config.WorkerCount
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
