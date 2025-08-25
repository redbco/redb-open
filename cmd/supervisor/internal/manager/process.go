package manager

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/redbco/redb-open/cmd/supervisor/internal/superconfig"
)

type ServiceProcess struct {
	name   string
	config superconfig.ServiceConfig
	cmd    *exec.Cmd
	mu     sync.Mutex
}

func NewServiceProcess(name string, config superconfig.ServiceConfig) *ServiceProcess {
	return &ServiceProcess{
		name:   name,
		config: config,
	}
}

func (p *ServiceProcess) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cmd != nil && p.cmd.Process != nil {
		return fmt.Errorf("process already running")
	}

	// Build command
	args := append([]string{}, p.config.Args...)
	p.cmd = exec.CommandContext(ctx, p.config.Executable, args...)

	// Set environment
	p.cmd.Env = os.Environ()
	for k, v := range p.config.Environment {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	// Add EXTERNAL_PORT environment variable if configured
	if p.config.ExternalPort > 0 {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("EXTERNAL_PORT=%d", p.config.ExternalPort))
	}

	// Add database configuration from supervisor config
	// This allows microservices to access the database name from environment variables
	if databaseName := os.Getenv("REDB_DATABASE_NAME"); databaseName != "" {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_DATABASE_NAME=%s", databaseName))
	}

	// Set output
	p.cmd.Stdout = os.Stdout
	p.cmd.Stderr = os.Stderr

	// Start process
	if err := p.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start process: %w", err)
	}

	// Monitor process in background
	go p.monitor()

	return nil
}

func (p *ServiceProcess) Stop(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cmd == nil || p.cmd.Process == nil {
		return nil
	}

	// Send interrupt signal
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		return fmt.Errorf("failed to send interrupt: %w", err)
	}

	// Wait for graceful shutdown
	done := make(chan error, 1)
	go func() {
		done <- p.cmd.Wait()
	}()

	select {
	case <-time.After(30 * time.Second):
		// Force kill
		p.cmd.Process.Kill()
		return fmt.Errorf("process did not exit gracefully")
	case err := <-done:
		return err
	}
}

func (p *ServiceProcess) monitor() {
	if err := p.cmd.Wait(); err != nil {
		// Handle process exit
		// Could implement restart logic here based on restart policy
	}
}
