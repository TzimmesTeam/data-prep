package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type URI struct {
	Bucket string
	Key    string
}

func ParseURI(value string) (URI, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return URI{}, fmt.Errorf("invalid S3 URI: %w", err)
	}
	if parsed.Scheme != "s3" || parsed.Host == "" || parsed.Path == "" || parsed.Path == "/" {
		return URI{}, fmt.Errorf("S3 URI must have s3://bucket/key form")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return URI{}, fmt.Errorf("S3 URI must not contain user info, query or fragment")
	}
	return URI{Bucket: parsed.Host, Key: strings.TrimPrefix(parsed.Path, "/")}, nil
}

func (u URI) String() string {
	return (&url.URL{Scheme: "s3", Host: u.Bucket, Path: "/" + u.Key}).String()
}

func (u URI) Join(elements ...string) URI {
	parts := append([]string{u.Key}, elements...)
	u.Key = path.Join(parts...)
	return u
}

type Config struct {
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UsePathStyle    bool
	Buckets         []string
}

type Storage struct {
	client  *awss3.Client
	buckets []string
}

func New(ctx context.Context, cfg Config) (*Storage, error) {
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AccessKeyID != "" {
		options = append(options, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := awss3.NewFromConfig(awsCfg, func(options *awss3.Options) {
		options.UsePathStyle = cfg.UsePathStyle
		if cfg.Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return &Storage{client: client, buckets: cfg.Buckets}, nil
}

func (s *Storage) CheckConfiguredBuckets(ctx context.Context) error {
	for _, bucket := range s.buckets {
		if err := s.CheckBucket(ctx, URI{Bucket: bucket}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) CheckBucket(ctx context.Context, uri URI) error {
	if _, err := s.client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(uri.Bucket)}); err != nil {
		return fmt.Errorf("head bucket %s: %w", uri.Bucket, err)
	}
	return nil
}

func (s *Storage) HeadSource(ctx context.Context, uri URI) (models.Source, error) {
	output, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(uri.Bucket),
		Key:    aws.String(uri.Key),
	})
	if err != nil {
		return models.Source{}, fmt.Errorf("head source %s: %w", uri.String(), err)
	}

	versionID := strings.TrimSpace(aws.ToString(output.VersionId))
	etag := strings.Trim(aws.ToString(output.ETag), "\"")
	revision := versionID
	if revision == "" || revision == "null" {
		versionID = ""
		revision = etag
	}
	if revision == "" {
		return models.Source{}, fmt.Errorf("source %s has neither VersionId nor ETag", uri.String())
	}
	return models.Source{
		URI:       uri.String(),
		Revision:  revision,
		VersionID: versionID,
		ETag:      etag,
		SizeBytes: aws.ToInt64(output.ContentLength),
	}, nil
}

func (s *Storage) OpenSource(ctx context.Context, uri URI, source models.Source) (io.ReadCloser, error) {
	getInput := &awss3.GetObjectInput{
		Bucket: aws.String(uri.Bucket),
		Key:    aws.String(uri.Key),
	}
	if source.VersionID != "" {
		getInput.VersionId = aws.String(source.VersionID)
	} else {
		getInput.IfMatch = aws.String(source.ETag)
	}
	output, err := s.client.GetObject(ctx, getInput)
	if err != nil {
		return nil, fmt.Errorf("get source %s: %w", uri.String(), err)
	}
	return output.Body, nil
}

func (s *Storage) Put(ctx context.Context, uri URI, body []byte, contentType string) error {
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(uri.Bucket),
		Key:         aws.String(uri.Key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("put object %s: %w", uri.String(), err)
	}
	return nil
}
