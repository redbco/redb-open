package manager

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redbco/redb-open/cmd/supervisor/internal/superconfig"
)

type ServiceProcess struct {
	name         string
	config       superconfig.ServiceConfig
	cmd          *exec.Cmd
	mu           sync.Mutex
	globalConfig *superconfig.Config
}

func NewServiceProcess(name string, config superconfig.ServiceConfig) *ServiceProcess {
	return &ServiceProcess{
		name:   name,
		config: config,
	}
}

func NewServiceProcessWithGlobalConfig(name string, config superconfig.ServiceConfig, globalConfig *superconfig.Config) *ServiceProcess {
	return &ServiceProcess{
		name:         name,
		config:       config,
		globalConfig: globalConfig,
	}
}

func (p *ServiceProcess) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cmd != nil && p.cmd.Process != nil {
		return fmt.Errorf("process already running")
	}

	// Build command with port offset applied
	args := p.applyPortOffsets(p.config.Args)
	p.cmd = exec.CommandContext(ctx, p.config.Executable, args...)

	// Set environment
	p.cmd.Env = os.Environ()
	for k, v := range p.config.Environment {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	// Add EXTERNAL_PORT environment variable if configured
	// External port is for external-facing services and should NOT be offset
	if p.config.ExternalPort > 0 {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("EXTERNAL_PORT=%d", p.config.ExternalPort))
	}

	// Add REST_API_PORT environment variable if configured
	// REST API port is external-facing and should NOT be offset
	if p.config.RestAPIPort > 0 {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REST_API_PORT=%d", p.config.RestAPIPort))
	}

	// Add database configuration from supervisor config
	// This allows microservices to access the database configuration from environment variables
	if p.globalConfig != nil {
		// Pass database name from supervisor config
		if p.globalConfig.Database.Name != "" {
			p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_DATABASE_NAME=%s", p.globalConfig.Database.Name))
		}
		// Pass database user from supervisor config
		if p.globalConfig.Database.User != "" {
			p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_DATABASE_USER=%s", p.globalConfig.Database.User))
		}

		// Pass keyring configuration for multi-instance support
		if p.globalConfig.Keyring.Backend != "" {
			p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_KEYRING_BACKEND=%s", p.globalConfig.Keyring.Backend))
		}
		if p.globalConfig.Keyring.Path != "" {
			p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_KEYRING_PATH=%s", p.globalConfig.Keyring.Path))
		}

		// Pass instance group ID for multi-instance isolation
		if p.globalConfig.InstanceGroup.GroupID != "" {
			p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_INSTANCE_GROUP_ID=%s", p.globalConfig.InstanceGroup.GroupID))
		}
	}

	// Also check environment variables as fallback
	if databaseName := os.Getenv("REDB_DATABASE_NAME"); databaseName != "" {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_DATABASE_NAME=%s", databaseName))
	}
	if databaseUser := os.Getenv("REDB_DATABASE_USER"); databaseUser != "" {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_DATABASE_USER=%s", databaseUser))
	}
	if keyringBackend := os.Getenv("REDB_KEYRING_BACKEND"); keyringBackend != "" {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_KEYRING_BACKEND=%s", keyringBackend))
	}
	if keyringPath := os.Getenv("REDB_KEYRING_PATH"); keyringPath != "" {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_KEYRING_PATH=%s", keyringPath))
	}
	if instanceGroupID := os.Getenv("REDB_INSTANCE_GROUP_ID"); instanceGroupID != "" {
		p.cmd.Env = append(p.cmd.Env, fmt.Sprintf("REDB_INSTANCE_GROUP_ID=%s", instanceGroupID))
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

func (p *ServiceProcess) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cmd != nil && p.cmd.Process != nil
}

// applyPortOffsets applies port offsets to service arguments for multi-instance support
func (p *ServiceProcess) applyPortOffsets(args []string) []string {
	if p.globalConfig == nil || p.globalConfig.InstanceGroup.PortOffset == 0 {
		return args
	}

	modifiedArgs := make([]string, len(args))
	copy(modifiedArgs, args)

	for i, arg := range modifiedArgs {
		// Handle --port=XXXX format (internal gRPC ports)
		if strings.HasPrefix(arg, "--port=") {
			portStr := strings.TrimPrefix(arg, "--port=")
			if port, err := strconv.Atoi(portStr); err == nil {
				newPort := p.globalConfig.ApplyPortOffset(port)
				modifiedArgs[i] = fmt.Sprintf("--port=%d", newPort)
			}
		}
		// Handle --supervisor=host:port format (internal communication)
		if strings.HasPrefix(arg, "--supervisor=") {
			supervisorStr := strings.TrimPrefix(arg, "--supervisor=")
			if strings.Contains(supervisorStr, ":") {
				parts := strings.Split(supervisorStr, ":")
				if len(parts) == 2 {
					if port, err := strconv.Atoi(parts[1]); err == nil {
						newPort := p.globalConfig.ApplyPortOffset(port)
						modifiedArgs[i] = fmt.Sprintf("--supervisor=%s:%d", parts[0], newPort)
					}
				}
			}
		}
		// Handle --grpc-bind=host:port format (internal gRPC)
		if strings.HasPrefix(arg, "--grpc-bind=") {
			bindStr := strings.TrimPrefix(arg, "--grpc-bind=")
			if strings.Contains(bindStr, ":") {
				parts := strings.Split(bindStr, ":")
				if len(parts) == 2 {
					if port, err := strconv.Atoi(parts[1]); err == nil {
						newPort := p.globalConfig.ApplyPortOffset(port)
						modifiedArgs[i] = fmt.Sprintf("--grpc-bind=%s:%d", parts[0], newPort)
					}
				}
			}
		}
		// Note: --listen is for external mesh port - NOT offset
		// External ports should be explicitly configured in the config file
	}

	return modifiedArgs
}
