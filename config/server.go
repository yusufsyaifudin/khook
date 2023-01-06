package config

import (
	"database/sql"
	"github.com/caarlos0/env"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/lib/pq"
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
	tmp := &ServerConfig{}
	err := env.Parse(tmp)
	if err != nil {
		return err
	}

	*s = *tmp
	if s.HTTP.Port <= 0 {
		s.HTTP.Port = 3333
	}

	return nil
}

type Postgres struct {
	DSN string `yaml:"dsn"`
}

func (p *Postgres) OpenConn(db *sql.DB, err error) {
	db, err = sql.Open("postgres", p.DSN)
	return
}
