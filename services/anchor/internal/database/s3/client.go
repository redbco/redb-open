package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// S3Client wraps the AWS S3 client with reDB-specific functionality.
type S3Client struct {
	client *s3.Client
	bucket string // Bucket name (treated as "database")
	region string
}

// NewS3Client creates a new S3 client from a database connection config.
func NewS3Client(ctx context.Context, cfg adapter.ConnectionConfig) (*S3Client, error) {
	// Build AWS config
	var awsCfg aws.Config
	var err error

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		// Use static credentials
		awsCfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(cfg.Region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				cfg.SessionToken,
			)),
		)
	} else {
		// Use default credentials chain
		awsCfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(cfg.Region),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Host != "" && cfg.Host != "s3.amazonaws.com" {
			// Custom endpoint (e.g., for MinIO or localstack)
			o.BaseEndpoint = aws.String(fmt.Sprintf("https://%s", cfg.Host))
			if cfg.Port > 0 && cfg.Port != 443 {
				o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port))
			}
		}
		if cfg.PathStyle {
			o.UsePathStyle = true
		}
	})

	return &S3Client{
		client: s3Client,
		bucket: cfg.DatabaseName, // In S3, bucket = database
		region: cfg.Region,
	}, nil
}

// NewS3ClientFromInstance creates a new S3 client from an instance config.
func NewS3ClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*S3Client, error) {
	// Convert to ConnectionConfig and create client
	connCfg := adapter.ConnectionConfig{
		Host:            cfg.Host,
		Port:            cfg.Port,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		SessionToken:    cfg.SessionToken,
		Region:          cfg.Region,
		PathStyle:       cfg.PathStyle,
	}

	return NewS3Client(ctx, connCfg)
}

// Ping tests the S3 connection by listing buckets.
func (c *S3Client) Ping(ctx context.Context) error {
	if c.bucket != "" {
		// Check if bucket exists
		_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(c.bucket),
		})
		return err
	}

	// Just list buckets to verify connectivity
	_, err := c.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	return err
}

// ListBuckets lists all S3 buckets.
func (c *S3Client) ListBuckets(ctx context.Context) ([]string, error) {
	result, err := c.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	buckets := make([]string, 0, len(result.Buckets))
	for _, bucket := range result.Buckets {
		if bucket.Name != nil {
			buckets = append(buckets, *bucket.Name)
		}
	}

	return buckets, nil
}

// CreateBucket creates a new S3 bucket.
func (c *S3Client) CreateBucket(ctx context.Context, name string, options map[string]interface{}) error {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(name),
	}

	// Add region configuration if not us-east-1
	if c.region != "" && c.region != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(c.region),
		}
	}

	_, err := c.client.CreateBucket(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	return nil
}

// DeleteBucket deletes an S3 bucket.
func (c *S3Client) DeleteBucket(ctx context.Context, name string) error {
	_, err := c.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	return nil
}

// GetBucket returns the current bucket name.
func (c *S3Client) GetBucket() string {
	return c.bucket
}

// Client returns the underlying AWS S3 client.
func (c *S3Client) Client() *s3.Client {
	return c.client
}
