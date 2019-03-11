import { sqlSmallQuery } from '../lib/sql-small-query'

const q1 = `
-- namespaces
CREATE SCHEMA IF NOT EXISTS :schemaName_data;
CREATE SCHEMA IF NOT EXISTS :schemaName_catalog;

-- exension for uuid
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- queue index table
CREATE TABLE IF NOT EXISTS ":schemaName_catalog"."fq_queues" (
    subject character varying(50) PRIMARY KEY,
    max_attempts integer DEFAULT 5,
    lock_duration character varying(20) DEFAULT '5m',
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);

-- queue maintenance table
CREATE TABLE IF NOT EXISTS ":schemaName_catalog"."fq_tasks" (
    subject character varying(50) PRIMARY KEY,
    payload jsonb DEFAULT '{}',
    status int DEFAULT 0,
    attempts integer DEFAULT 0,
    iterations integer DEFAULT 0,
    next_iteration timestamp with time zone,
    last_iteration timestamp with time zone
);



--
-- SETTINGS TRIGGERS
-- Every time a queue setting change we will refresh the Fetchq client
-- in-memory data rapresentation (it keeps a json version of the rows)
--
-- This is used to automatically build queries like the maintenance for
-- orphans and dead that are dependent on the "max_attempt" setting.
--

CREATE OR REPLACE FUNCTION fetchq_fq_queues_insert_trigger_fn()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('fetchq_settings', row_to_json(NEW)::text);
    RETURN NULL;
END; $$ LANGUAGE 'plpgsql';

DROP TRIGGER IF EXISTS fetchq_fq_queues_insert_trigger ON ":schemaName_catalog"."fq_queues";
CREATE TRIGGER fetchq_fq_queues_insert_trigger AFTER INSERT
ON ":schemaName_catalog"."fq_queues" FOR EACH ROW
EXECUTE PROCEDURE fetchq_fq_queues_insert_trigger_fn();

CREATE OR REPLACE FUNCTION fetchq_fq_queues_update_trigger_fn()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('fetchq_settings', row_to_json(NEW)::text);
    RETURN NULL;
END; $$ LANGUAGE 'plpgsql';

DROP TRIGGER IF EXISTS fetchq_fq_queues_update_trigger ON ":schemaName_catalog"."fq_queues";
CREATE TRIGGER fetchq_fq_queues_update_trigger AFTER UPDATE
ON ":schemaName_catalog"."fq_queues" FOR EACH ROW
EXECUTE PROCEDURE fetchq_fq_queues_update_trigger_fn();
`

export default ctx => {
    const [ _q1 ] = sqlSmallQuery(ctx, q1)

    return async () => {
        try {
            await ctx.query(_q1)
        } catch (err) {
            const error = new Error(`[Fetchq] failed to init schema: ${ctx.schema} - ${err.message}`)
            error.original = err
            throw error
        }
    }    
}