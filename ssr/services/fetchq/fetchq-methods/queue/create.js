import { sqlSmallQuery } from '../lib/sql-small-query'
import createIndex from './index-create'

const q = `
CREATE TABLE ":schemaName_data".":queueName__docs" (
    subject character varying(50) PRIMARY KEY,
    payload jsonb DEFAULT '{}',
    status int DEFAULT -1,
    attempts integer DEFAULT 0,
    iterations integer DEFAULT 0,
    next_iteration timestamp with time zone,
    last_iteration timestamp with time zone
);

CREATE TABLE ":schemaName_data".":queueName__metrics" (
    metric character varying(50) PRIMARY KEY,
    amount integer DEFAULT 0,
    last_update timestamp with time zone
);

-- list the new queue in the catalog index
INSERT INTO ":schemaName_catalog"."fq_queues"
( subject, created_at )
VALUES
( ':queueName', NOW() );

-- push maintenance tasks
INSERT INTO ":schemaName_catalog"."fq_tasks"
( subject, payload, next_iteration )
VALUES
( ':queueName:mnt', '{}', NOW() );
`

export default ctx => {
    const [ _q ] = sqlSmallQuery(ctx, q)

    return async (queueName, options = {}) => {
        try {
            await ctx.query(_q
                .replace(/:queueName/g, queueName)
            )
    
            if (options.index !== false) {
                await createIndex(ctx)(queueName)
            }
    
        } catch (err) {
            if (!err.original || err.original.code !== '42P07') {
                const error = new Error(`[Fetchq] failed to create queue: ${queueName} - ${err.message}`)
                error.original = err
                throw error
            }
        }
    }
    
}