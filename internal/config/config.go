package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env            string        `yaml:"env" env-default:"local"`
	StartupTimeout time.Duration `yaml:"startup_timeout"`
	HTTP           HTTP          `yaml:"http"`
	Batch          Batch         `yaml:"batch"`
	S3             S3            `yaml:"s3"`
	JetStream      JetStream     `yaml:"jetstream"`
}

type HTTP struct {
	Port            int           `yaml:"port"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type Batch struct {
	MaxRows          int `yaml:"max_rows"`
	PartitionRows    int `yaml:"partition_rows"`
	PartitionBytes   int `yaml:"partition_bytes"`
	RejectedLogLimit int `yaml:"rejected_log_limit"`
}

type S3 struct {
	Region          string   `yaml:"region"`
	Endpoint        string   `yaml:"endpoint"`
	AccessKeyID     string   `yaml:"access_key_id" env:"S3_ACCESS_KEY_ID"`
	SecretAccessKey string   `yaml:"secret_access_key" env:"S3_SECRET_ACCESS_KEY"`
	UsePathStyle    bool     `yaml:"use_path_style"`
	Buckets         []string `yaml:"buckets" env:"S3_BUCKETS" env-separator:","`
}

type JetStream struct {
	URL             string        `yaml:"url" env:"NATS_URL"`
	TasksStream     string        `yaml:"tasks_stream"`
	ResultsStream   string        `yaml:"results_stream"`
	TaskSubject     string        `yaml:"task_subject"`
	ResultSubject   string        `yaml:"result_subject"`
	FailedSubject   string        `yaml:"failed_subject"`
	ConsumerDurable string        `yaml:"consumer_durable"`
	Parallelism     int           `yaml:"parallelism"`
	DeliveryLimit   int           `yaml:"delivery_limit"`
	DialTimeout     time.Duration `yaml:"dial_timeout"`
	ReconnectDelay  time.Duration `yaml:"reconnect_delay"`
	FetchWait       time.Duration `yaml:"fetch_wait"`
	AckWait         time.Duration `yaml:"ack_wait"`
	TaskTimeout     time.Duration `yaml:"task_timeout"`
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
	if c.StartupTimeout <= 0 || c.HTTP.ShutdownTimeout <= 0 {
		return fmt.Errorf("startup and shutdown timeouts must be positive")
	}
	if c.HTTP.Port < 1 || c.HTTP.Port > 65535 {
		return fmt.Errorf("invalid HTTP port")
	}
	if c.Batch.MaxRows <= 0 || c.Batch.PartitionRows <= 0 || c.Batch.PartitionBytes <= 0 || c.Batch.RejectedLogLimit < 0 {
		return fmt.Errorf("invalid batch limits")
	}
	if c.S3.Region == "" || c.S3.AccessKeyID == "" || c.S3.SecretAccessKey == "" || len(c.S3.Buckets) == 0 {
		return fmt.Errorf("incomplete S3 config")
	}
	if c.JetStream.URL == "" {
		return fmt.Errorf("NATS URL is empty")
	}
	if c.JetStream.TasksStream == "" || c.JetStream.ResultsStream == "" ||
		c.JetStream.TaskSubject == "" || c.JetStream.ResultSubject == "" ||
		c.JetStream.FailedSubject == "" || c.JetStream.ConsumerDurable == "" {
		return fmt.Errorf("incomplete JetStream topology config")
	}
	if c.JetStream.Parallelism <= 0 || c.JetStream.DeliveryLimit <= 0 ||
		c.JetStream.DialTimeout <= 0 || c.JetStream.ReconnectDelay <= 0 ||
		c.JetStream.FetchWait <= 0 || c.JetStream.AckWait <= 0 || c.JetStream.TaskTimeout <= 0 {
		return fmt.Errorf("invalid JetStream limits or timeouts")
	}
	if c.JetStream.AckWait <= c.JetStream.TaskTimeout {
		return fmt.Errorf("JetStream ack_wait must exceed task_timeout")
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
