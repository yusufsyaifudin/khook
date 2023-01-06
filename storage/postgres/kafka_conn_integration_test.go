package postgres

import (
	"context"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/yusufsyaifudin/khook/config"
	"testing"
	"time"
)

func TestItgKafkaConfigRows(t *testing.T) {
	cfg := config.TestConfig{}
	err := cfg.Load()
	assert.NoError(t, err)

	if !cfg.EnableIntegrationTest {
		t.Skip()
	}

	db, err := sql.Open("postgres", cfg.PostgresDSN)
	assert.NotNil(t, db)
	assert.NoError(t, err)

	conn, err := NewKafkaConnStore(WithDB(db))
	assert.NotNil(t, conn)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := conn.GetKafkaConfigs(ctx)
	assert.NoError(t, err)

	for rows.Next() {
		kafkaCfg, checksum, err := rows.KafkaConfig()
		t.Log(kafkaCfg, checksum, err)
	}
}
