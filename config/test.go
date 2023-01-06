package config

import (
	"fmt"
	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
)

type TestConfig struct {
	EnableIntegrationTest bool   `env:"ENABLE_INTEGRATION_TEST"`
	PostgresDSN           string `env:"TEST_POSTGRES_DSN"`
}

func (t *TestConfig) Load() error {
	err := godotenv.Load("test.env")
	if err != nil {
		err = fmt.Errorf("cannot load test.env file: %w", err)
		return err
	}

	tmp := &TestConfig{}
	err = env.Parse(tmp)
	if err != nil {
		return err
	}

	*t = *tmp
	if t.PostgresDSN == "" {
		t.PostgresDSN = "user=postgres password=postgres host=localhost port=5433 dbname=khook sslmode=disable"
	}

	return nil
}
