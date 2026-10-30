package config

import (
	"errors"
	"flag"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env   string `yaml:"env" env-default:"local"`
	HTTP  HTTP   `yaml:"http"`
	Batch Batch  `yaml:"batch"`
}

type HTTP struct {
	Port int `yaml:"port"`
}

type Batch struct {
	MaxSize       int `yaml:"max_size"`
	PartitionSize int `yaml:"partition_size"`
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

	return &cfg
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
