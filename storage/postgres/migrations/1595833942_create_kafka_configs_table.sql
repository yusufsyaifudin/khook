-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
CREATE TABLE IF NOT EXISTS kafka_configs (
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    namespace VARCHAR NOT NULL,
    spec JSONB NOT NULL DEFAULT '{}',

    -- using unix microsecond to make it easier to migrate between db
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()) * 1000000),
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()) * 1000000)
);

-- INSERT INTO kafka_configs(id, name, namespace, spec, created_at, updated_at) (SELECT x, concat('kafka-', x::text), 'default', concat('{"apiVersion":"khook/v1","kind":"KafkaBrokerConnection","namespace":"default","name":"',  x::text, '","spec":{"brokers":["localhost:9092"]}}')::jsonb, extract(epoch from now()), extract(epoch from now()) FROM generate_series(1,100) AS x);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_kafka_configs_name_namespace_rev ON kafka_configs (name, namespace, rev);

-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
DROP TABLE IF EXISTS kafka_configs;
