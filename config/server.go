package config

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
	"os"
)

type ServerConfig struct {
	HTTP struct {
		Port int `yaml:"port"`
	} `yaml:"http"`

	Storage struct {
		KafkaConnStore struct {
			Postgres *Postgres `yaml:"postgres"`
		} `yaml:"kafka_conn_store"`
	} `yaml:"storage"`
}

func (s *ServerConfig) Load() error {
	const configFileName = "config.yaml"
	fileContent, err := os.ReadFile(configFileName)
	if err != nil {
		err = fmt.Errorf("error read file config %s: %w", configFileName, err)
		return err
	}

	cfg := &ServerConfig{}
	dec := yaml.NewDecoder(bytes.NewReader(fileContent))
	dec.KnownFields(false)
	err = dec.Decode(&cfg)
	if err != nil {
		return err
	}

	*s = *cfg
	if s.HTTP.Port <= 0 {
		s.HTTP.Port = 3333
	}

	return nil
}

type Postgres struct {
	DSN string `yaml:"dsn"`
}

func (p *Postgres) OpenConn() (db *sql.DB, err error) {
	db, err = sql.Open("postgres", p.DSN)
	return
}
