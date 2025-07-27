package grpc

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ClientOptions contains options for creating a gRPC client
type ClientOptions struct {
	// Keepalive parameters
	KeepaliveTime    time.Duration
	KeepaliveTimeout time.Duration

	// Connection timeout
	DialTimeout time.Duration

	// Additional dial options
	DialOptions []grpc.DialOption
}

// DefaultClientOptions returns default client options
func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		KeepaliveTime:    10 * time.Second,
		KeepaliveTimeout: 3 * time.Second,
		DialTimeout:      10 * time.Second,
	}
}

// NewClient creates a new gRPC client connection
func NewClient(ctx context.Context, addr string, opts ClientOptions) (*grpc.ClientConn, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                opts.KeepaliveTime,
			Timeout:             opts.KeepaliveTimeout,
			PermitWithoutStream: true,
		}),
	}

	dialOpts = append(dialOpts, opts.DialOptions...)

	if opts.DialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.DialTimeout)
		defer cancel()
		dialOpts = append(dialOpts, grpc.WithBlock())
	}

	return grpc.DialContext(ctx, addr, dialOpts...)
}
