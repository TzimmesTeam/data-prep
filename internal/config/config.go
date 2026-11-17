package config

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env            string        `yaml:"env" env-default:"local"`
	StartupTimeout time.Duration `yaml:"startup_timeout"`
	HTTP           HTTP          `yaml:"http"`
	S3             S3            `yaml:"s3"`
	SQS            SQS           `yaml:"sqs"`
	ControlPlane   ControlPlane  `yaml:"control_plane"`
}

type HTTP struct {
	Port            int           `yaml:"port"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type S3 struct {
	Region           string `yaml:"region"`
	Endpoint         string `yaml:"endpoint"`
	AccessKeyID      string `yaml:"access_key_id" env:"S3_ACCESS_KEY_ID"`
	SecretAccessKey  string `yaml:"secret_access_key" env:"S3_SECRET_ACCESS_KEY"`
	UsePathStyle     bool   `yaml:"use_path_style"`
	RawBucket        string `yaml:"raw_bucket" env:"S3_RAW_BUCKET"`
	CuratedBucket    string `yaml:"curated_bucket" env:"S3_CURATED_BUCKET"`
	QuarantineBucket string `yaml:"quarantine_bucket" env:"S3_QUARANTINE_BUCKET"`
}

type SQS struct {
	Region            string        `yaml:"region"`
	Endpoint          string        `yaml:"endpoint"`
	QueueURL          string        `yaml:"queue_url" env:"SQS_QUEUE_URL"`
	AccessKeyID       string        `yaml:"access_key_id" env:"SQS_ACCESS_KEY_ID"`
	SecretAccessKey   string        `yaml:"secret_access_key" env:"SQS_SECRET_ACCESS_KEY"`
	WaitTimeSeconds   int32         `yaml:"wait_time_seconds"`
	VisibilityTimeout int32         `yaml:"visibility_timeout"`
	ReconnectDelay    time.Duration `yaml:"reconnect_delay"`
}

type ControlPlane struct {
	URL         string        `yaml:"url" env:"CONTROL_PLANE_URL"`
	Timeout     time.Duration `yaml:"timeout"`
	MaxAttempts int           `yaml:"max_attempts"`
	RetryDelay  time.Duration `yaml:"retry_delay"`
}

func MustLoad() *Config {
	configPath := fetchConfigPath()
	if configPath == "" {
		panic("config path is empty")
	}
	if _, err := os.Stat(configPath); errors.Is(err, os.ErrNotExist) {
		panic("config file does not exist: " + configPath)
	}

	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		panic("failed to load config: " + err.Error())
	}
	if err := cfg.Validate(); err != nil {
		panic("invalid config: " + err.Error())
	}
	return &cfg
}

func (c *Config) Validate() error {
	validators := []func() error{
		c.validateHTTP,
		c.validateS3,
		c.validateSQS,
		c.validateControlPlane,
	}
	for _, validate := range validators {
		if err := validate(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) validateHTTP() error {
	if c.StartupTimeout <= 0 || c.HTTP.ShutdownTimeout <= 0 {
		return fmt.Errorf("startup and shutdown timeouts must be positive")
	}
	if c.HTTP.Port < 1 || c.HTTP.Port > 65535 {
		return fmt.Errorf("invalid HTTP port")
	}
	return nil
}

func (c *Config) validateS3() error {
	if c.S3.Region == "" || c.S3.AccessKeyID == "" || c.S3.SecretAccessKey == "" ||
		c.S3.RawBucket == "" || c.S3.CuratedBucket == "" || c.S3.QuarantineBucket == "" {
		return fmt.Errorf("incomplete S3 config")
	}
	return nil
}

func (c *Config) validateSQS() error {
	if c.SQS.Region == "" || c.SQS.QueueURL == "" || c.SQS.AccessKeyID == "" || c.SQS.SecretAccessKey == "" {
		return fmt.Errorf("incomplete SQS config")
	}
	if c.SQS.WaitTimeSeconds < 0 || c.SQS.WaitTimeSeconds > 20 || c.SQS.VisibilityTimeout <= 0 || c.SQS.ReconnectDelay <= 0 {
		return fmt.Errorf("invalid SQS polling config")
	}
	return nil
}

func (c *Config) validateControlPlane() error {
	if _, err := url.ParseRequestURI(c.ControlPlane.URL); err != nil {
		return fmt.Errorf("invalid control plane URL: %w", err)
	}
	if c.ControlPlane.Timeout <= 0 || c.ControlPlane.MaxAttempts <= 0 || c.ControlPlane.RetryDelay <= 0 {
		return fmt.Errorf("invalid control plane client config")
	}
	return nil
}

func fetchConfigPath() string {
	var path string
	flag.StringVar(&path, "config", "", "path to config file")
	flag.Parse()
	if path == "" {
		path = os.Getenv("CONFIG_PATH")
	}
	return path
}
