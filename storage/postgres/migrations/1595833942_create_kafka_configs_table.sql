-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
CREATE TABLE IF NOT EXISTS kafka_configs (
    id BIGINT NOT NULL PRIMARY KEY,
    label VARCHAR NOT NULL,
    config JSONB NOT NULL DEFAULT '{}', -- credential based on service_provider type
    config_checksum VARCHAR NOT NULL, -- this to know whether the hash is different or not after update

    -- using unix microsecond to make it easier to migrate between db
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()) * 1000000),
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()) * 1000000),

    -- ensure that this only one record that not deleted
    deleted_at BIGINT NOT NULL DEFAULT 0
);

-- unique constraint prevented at sql level
-- https://dba.stackexchange.com/a/9760
-- label | deleted_at
-- a     | 2022-10-10 10:04:00 -> OK
-- a     | 2022-10-10 10:05:00 -> OK
-- a     | 1970-01-01 00:00:00 -> OK
-- a     | 1970-01-01 00:00:00 -> FAILED because previous data has NULL 1970-01-01 00:00:00
-- We're not using NULL since it need more handling (both in SQL level or Code level)!
-- We assume that record with or below date 1970-01-01 00:00:00 is valid, and greater than that date is deleted (tombstone).
-- Try use:
-- INSERT INTO kafka_configs(id, label, config, config_checksum, created_at, updated_at) (SELECT x, concat('kafka-', x::text), concat('{"brokers":["localhost:9092"],"label":"kafka-',  x::text, '"}')::jsonb,  encode(sha256(convert_to(concat('{"brokers":["localhost:9092"],"label":"kafka-',  x::text, '"}'), 'UTF8')), 'hex'), extract(epoch from now()), extract(epoch from now()) FROM generate_series(1,100) AS x);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_kafka_configs_label_deleted ON kafka_configs (LOWER(label), deleted_at);

-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
DROP TABLE IF EXISTS kafka_configs;
