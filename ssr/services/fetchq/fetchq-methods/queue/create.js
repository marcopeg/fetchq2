import { sqlSmallQuery } from '../lib/sql-small-query'

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
( subject, created_at :max_attempt_head :lock_duration_head )
VALUES
( ':queueName', NOW() :max_attempt_value :lock_duration_value );

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
            let query = _q.replace(/:queueName/g, queueName)

            if (options.max_attempts !== undefined) {
                query = query
                    .replace(/:max_attempt_head/g, ', max_attempts')
                    .replace(/:max_attempt_value/g, `, ${Number(options.max_attempts)}`)
            }
            
            if (options.lock_duration) {
                query = query
                    .replace(/:lock_duration_head/g, ', lock_duration')
                    .replace(/:lock_duration_value/g, `, '${options.lock_duration}'`)
            }

            query = query
                .replace(/:max_attempt_head/g, '')
                .replace(/:max_attempt_value/g, '')
                .replace(/:lock_duration_head/g, '')
                .replace(/:lock_duration_value/g, '')

            await ctx.query(query)
    
            if (options.index !== false) {
                await ctx.queue.index(queueName)
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