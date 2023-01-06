package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/yusufsyaifudin/khook/storage"
	"io"
	"sync"
	"time"
)

const (
	SqlPersist = `
INSERT INTO kafka_configs (id, label, KafkaConnStoreConfig, config_checksum, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6) 
ON CONFLICT (LOWER(label), deleted_at) 
DO UPDATE SET 
   KafkaConnStoreConfig = EXCLUDED.KafkaConnStoreConfig,
   config_checksum = EXCLUDED.config_checksum,
   updated_at = EXCLUDED.updated_at 
WHERE LOWER(apps.client_id) = $2 AND apps.deleted_at = 0 
RETURNING *;
`

	SqlGetLastID    = `SELECT id FROM kafka_configs WHERE deleted_at = 0 ORDER BY id DESC LIMIT 1;`
	SqlGetRowsRange = `SELECT * FROM kafka_configs WHERE id > $1 AND id <= $2 AND deleted_at = 0 ORDER BY id ASC LIMIT 20;`
	SqlSoftDelete   = `UPDATE FROM kafka_configs SET deleted_at = $2 WHERE LOWER(label) = $1 AND deleted_at = 0 LIMIT 1;`
)

type KafkaConfigTable struct {
	ID             int64               `db:"id"`
	Label          string              `db:"label"`
	Config         storage.KafkaConfig `db:"config"`
	ConfigChecksum string              `db:"config_checksum"`

	// Timestamp using integer as unix microsecond in UTC
	CreatedAt int64 `db:"created_at"`
	UpdatedAt int64 `db:"updated_at"`
	DeletedAt int64 `db:"deleted_at"`
}

type Option func(*KafkaConnStoreConfig) error

type KafkaConnStoreConfig struct {
	DB *sql.DB
}

func defaultConfig() *KafkaConnStoreConfig {
	return &KafkaConnStoreConfig{}
}

func WithDB(db *sql.DB) Option {
	return func(c *KafkaConnStoreConfig) error {
		if db == nil {
			return fmt.Errorf("empty db")
		}

		c.DB = db
		return nil
	}
}

type KafkaConnStore struct {
	config *KafkaConnStoreConfig
	db     *sqlx.DB
}

var _ storage.KafkaConnStore = (*KafkaConnStore)(nil)

func NewKafkaConnStore(opts ...Option) (*KafkaConnStore, error) {

	cfg := defaultConfig()
	for _, opt := range opts {
		err := opt(cfg)
		if err != nil {
			return nil, err
		}
	}

	dbConn := sqlx.NewDb(cfg.DB, "postgres")
	if dbConn == nil {
		return nil, fmt.Errorf("nil sqlx.DB on postgres kafka connection store")
	}

	err := dbConn.Ping()
	if err != nil {
		err = fmt.Errorf("db ping error: %w", err)
		return nil, err
	}

	return &KafkaConnStore{
		config: cfg,
		db:     dbConn,
	}, nil
}

func (k *KafkaConnStore) PersistKafkaConfig(ctx context.Context, in storage.InputPersistKafkaConfig) (out storage.OutPersistKafkaConfig, err error) {
	kafkaCfg, err := json.Marshal(in.KafkaConfig)
	if err != nil {
		err = fmt.Errorf("cannot marshal Kafka Config to json: %w", err)
		return
	}

	id := 123

	h := sha256.New()
	_, err = io.Copy(h, bytes.NewReader(kafkaCfg))
	if err != nil {
		err = fmt.Errorf("cannot copy to calculate KafkaConnStoreConfig checksum: %w", err)
		return
	}

	checkSum := hex.EncodeToString(h.Sum(nil))
	nowMicro := time.Now().UnixMicro()

	var kafkaConfigOut KafkaConfigTable
	err = sqlx.SelectContext(ctx, k.db, &kafkaConfigOut, SqlPersist,
		id, in.KafkaConfig.Label, kafkaCfg, checkSum, nowMicro, nowMicro,
	)
	if err != nil {
		err = fmt.Errorf("cannot exec query: %s", err)
		return
	}

	out = storage.OutPersistKafkaConfig{
		KafkaConfig: kafkaConfigOut.Config,
		CheckSum:    kafkaConfigOut.ConfigChecksum,
	}
	return
}

func (k *KafkaConnStore) GetAllKafkaConfig(ctx context.Context) (rows storage.KafkaConfigRows, err error) {
	lastRow := struct {
		LastID int64 `db:"id"`
	}{}
	err = sqlx.GetContext(ctx, k.db, &lastRow, SqlGetLastID)
	if errors.Is(err, sql.ErrNoRows) {
		rows = &storage.NoRows{}
		err = nil
		return
	}

	if err != nil {
		err = fmt.Errorf("cannot get last row: %w", err)
		return
	}

	rows = &KafkaConfigRows{
		ctx:      ctx,
		lastID:   lastRow.LastID,
		db:       k.db,
		currRows: make([]KafkaConfigTable, 0),
	}

	return
}

func (k *KafkaConnStore) DeleteKafkaConfig(ctx context.Context, in storage.InputDeleteKafkaConfig) (out storage.OutDeleteKafkaConfig, err error) {
	_, err = k.db.ExecContext(ctx, SqlSoftDelete, in.Label, time.Now().UnixMicro())
	if err != nil {
		err = fmt.Errorf("cannot delete from postgre: %w", err)
		return
	}

	out = storage.OutDeleteKafkaConfig{
		Success: true,
	}

	return
}

type KafkaConfigRows struct {
	ctx       context.Context
	lock      sync.RWMutex
	iter      int
	nextMinID int64
	lastID    int64
	db        *sqlx.DB

	// state management
	stopFetchDb bool
	currRow     KafkaConfigTable
	currRows    []KafkaConfigTable
	currRowsErr error
}

var _ storage.KafkaConfigRows = (*KafkaConfigRows)(nil)

func (k *KafkaConfigRows) Next() bool {
	k.lock.Lock()
	defer k.lock.Unlock()

	k.iter++

	if len(k.currRows) > 0 {
		k.currRow, k.currRows = k.currRows[0], k.currRows[1:]
		return true
	}

	if k.stopFetchDb {
		return false
	}

	var rows []KafkaConfigTable
	err := sqlx.SelectContext(k.ctx, k.db, &rows, SqlGetRowsRange, k.nextMinID, k.lastID)
	if err != nil {
		err = fmt.Errorf("cannot get rows for the iteration %d: %w", k.iter, err)

		// tell to stop query and return error
		k.currRowsErr = err
		k.stopFetchDb = true

		// return true, so the first call of Next will still has the chance to call the KafkaConfig.
		// But, since the KafkaConfig return error, user may break the loop,
		// or if not, then stopFetchDb will stop the next iteration.
		return true
	}

	if len(rows) <= 0 {
		return false
	}

	k.currRows = rows
	k.nextMinID = rows[len(rows)-1].ID

	// tell Next to stop fetch db if last id already fetched.
	// Use larger or equal operator, since the last ID may miss from list during fetch
	// (because delete operation)
	if k.nextMinID >= k.lastID {
		k.stopFetchDb = true
	}

	k.currRow, k.currRows = k.currRows[0], k.currRows[1:]
	return true
}

func (k *KafkaConfigRows) KafkaConfig() (storage.KafkaConfig, string, error) {
	k.lock.RLock()
	defer k.lock.RUnlock()

	if k.currRowsErr != nil {
		return storage.KafkaConfig{}, "", k.currRowsErr
	}

	return k.currRow.Config, k.currRow.ConfigChecksum, nil
}
