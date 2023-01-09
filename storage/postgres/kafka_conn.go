package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sony/sonyflake"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"github.com/yusufsyaifudin/khook/storage"
	"sync"
	"time"
)

const (
	SqlPersist = `
INSERT INTO kafka_configs (id, name, namespace, rev, spec, status, created_at, updated_at) 
VALUES ($1, $2, $3, $4, $5, $6, $7, $8) 
RETURNING *;
`

	SqlGetOne       = `SELECT * FROM kafka_configs WHERE name = $1 AND namespace $2 ORDER BY rev DESC LIMIT 1;`
	SqlGetLastID    = `SELECT id FROM kafka_configs ORDER BY id DESC LIMIT 1;`
	SqlGetRowsRange = `SELECT * FROM kafka_configs WHERE id > $1 AND id <= $2 ORDER BY id ASC LIMIT 20;`
)

type KafkaConfigTable struct {
	ID int64 `db:"id"` // Internal Postgres ID

	// These field is come from storage.Metadata
	// Unique index Name and Namespace and Rev must be created to ensure that we can increment the Rev counter
	Name      string `db:"name"`
	Namespace string `db:"namespace"`
	Rev       int    `db:"rev"`

	// Spec store all configuration copy of the Object
	Spec storage.KafkaConnectionConfig `db:"spec"`

	// These field is come from storage.ResourceState
	Status storage.Status `db:"status"`

	// Timestamp using integer as unix microsecond in UTC
	CreatedAt int64 `db:"created_at"`
	UpdatedAt int64 `db:"updated_at"`
}

type Option func(*KafkaConnStoreConfig) error

type KafkaConnStoreConfig struct {
	DB *sql.DB
	SF *sonyflake.Sonyflake
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

func WithSonyFlake(sf *sonyflake.Sonyflake) Option {
	return func(c *KafkaConnStoreConfig) error {
		if sf == nil {
			return fmt.Errorf("empty sonyflake instance")
		}

		c.SF = sf
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

func (k *KafkaConnStore) PersistKafkaConnConfig(ctx context.Context, in storage.InPersistKafkaConnConfig) (out storage.OutPersistKafkaConnConfig, err error) {
	err = validator.Validate(in)
	if err != nil {
		err = fmt.Errorf("postgres: input validation error: %w", err)
		return
	}

	kafkaCfg, err := json.Marshal(in.KafkaConfig)
	if err != nil {
		err = fmt.Errorf("cannot marshal Kafka Config to json: %w", err)
		return
	}

	id, err := k.config.SF.NextID()
	if err != nil {
		err = fmt.Errorf("cannot generate distributed unique id: %w", err)
		return
	}

	resourceState := in.ResourceState

	var kafkaConfigOut KafkaConfigTable
	err = sqlx.GetContext(ctx, k.db, &kafkaConfigOut, SqlPersist,
		id, in.KafkaConfig.Name, in.KafkaConfig.Namespace, in.ResourceState.Rev, kafkaCfg,
		resourceState.Status,
		resourceState.CreatedAt.UnixMicro(), resourceState.UpdatedAt.UnixMicro(),
	)
	if err != nil {
		err = fmt.Errorf("cannot exec query: %s", err)
		return
	}

	out = storage.OutPersistKafkaConnConfig{
		KafkaConfig: kafkaConfigOut.Spec,
		ResourceState: storage.ResourceState{
			Rev:       kafkaConfigOut.Rev,
			Status:    kafkaConfigOut.Status,
			CreatedAt: time.UnixMicro(kafkaConfigOut.CreatedAt),
			UpdatedAt: time.UnixMicro(kafkaConfigOut.UpdatedAt),
		},
	}
	return
}

func (k *KafkaConnStore) GetKafkaConnConfig(ctx context.Context, in storage.InGetKafkaConnConfig) (out storage.OutGetKafkaConnConfig, err error) {

	var row KafkaConfigTable
	err = sqlx.GetContext(ctx, k.db, &row, SqlGetOne, in.Name, in.Namespace)
	if err != nil {
		err = fmt.Errorf("cannot resource for name '%s' ns '%s': %w", in.Name, in.Namespace, err)
		return
	}

	out = storage.OutGetKafkaConnConfig{
		KafkaConfig: row.Spec,
		ResourceState: storage.ResourceState{
			Rev:       row.Rev,
			Status:    row.Status,
			CreatedAt: time.UnixMicro(row.CreatedAt),
			UpdatedAt: time.UnixMicro(row.UpdatedAt),
		},
	}
	return
}

func (k *KafkaConnStore) GetKafkaConnConfigs(ctx context.Context) (rows storage.KafkaConnConfigRows, err error) {
	lastRow := struct {
		LastID int64 `db:"id"`
	}{}
	err = sqlx.GetContext(ctx, k.db, &lastRow, SqlGetLastID)
	if errors.Is(err, sql.ErrNoRows) {
		rows = &storage.KafkaConfigNoRows{}
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

var _ storage.KafkaConnConfigRows = (*KafkaConfigRows)(nil)

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

		// return true, so the first call of Next will still has the chance to call the KafkaConnectionConfig.
		// But, since the KafkaConnectionConfig return error, user may break the loop,
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

func (k *KafkaConfigRows) KafkaConnection() (storage.KafkaConnectionConfig, storage.ResourceState, error) {
	k.lock.RLock()
	defer k.lock.RUnlock()

	if k.currRowsErr != nil {
		return storage.KafkaConnectionConfig{}, storage.ResourceState{}, k.currRowsErr
	}

	return k.currRow.Spec, storage.ResourceState{
		Rev:       k.currRow.Rev,
		Status:    k.currRow.Status,
		CreatedAt: time.UnixMicro(k.currRow.CreatedAt),
		UpdatedAt: time.UnixMicro(k.currRow.UpdatedAt),
	}, nil
}
