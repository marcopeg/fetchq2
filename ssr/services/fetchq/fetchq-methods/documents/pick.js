
import { FetchqPlan } from '../utils/plan'
import { sqlSmallQuery } from '../lib/sql-small-query'

const q = `
WITH
picked_tasks AS (
    SELECT origin.* FROM ":schemaName_data".":queueName__docs" AS origin
    WHERE
        status = 1
    ORDER BY
        attempts ASC,
        next_iteration ASC
    LIMIT :limit
    FOR UPDATE FOR UPDATE SKIP LOCKED    
),
updated_tasks AS (
    UPDATE ":schemaName_data".":queueName__docs" AS target
    SET 
        status = 2,
        attempts = target.attempts + 1,
        next_iteration = :lock
    FROM (
        SELECT * FROM picked_tasks
    ) AS origin
    WHERE target.subject = origin.subject
    RETURNING
        target.*,
        origin.next_iteration AS prev_iteration,
        origin.attempts AS prev_attempts
)
:queryStats
SELECT
    subject,
    payload,
    status,
    attempts,
    iterations,
    prev_iteration,
    next_iteration,
    last_iteration
FROM updated_tasks ORDER BY
prev_attempts ASC,
prev_iteration ASC;
`

const qStats = `
,
increment_wip AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'wip', (SELECT COUNT(subject) FROM picked_tasks), NOW()
    ON CONFLICT (metric) DO
    UPDATE SET amount = t.amount + EXCLUDED.amount, last_update = EXCLUDED.last_update
),
decrement_pnd AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'pnd', (SELECT COUNT(subject) FROM picked_tasks), NOW()
    ON CONFLICT (metric) DO
    UPDATE SET amount = t.amount - EXCLUDED.amount, last_update = EXCLUDED.last_update
),
increment_pkd AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'pkd', (SELECT COUNT(subject) FROM picked_tasks), NOW()
    ON CONFLICT (metric) DO
    UPDATE SET amount = t.amount + EXCLUDED.amount, last_update = EXCLUDED.last_update
)
`

export default ctx => {
    const [ _q, _qStats ] = sqlSmallQuery(ctx, q, qStats)

    return async (queueName, limit = 1, options = {}) => {
        try {
            const query = _q
                .replace(/:queueName/g, queueName)
                .replace(/:limit/g, Number(limit))
                .replace(/:lock/g, (new FetchqPlan(options.lock || '5m')).toString())
                .replace(/:queryStats/g, options.metrics === false ? '' : (
                    _qStats.replace(/:queueName/g, queueName)
                ))

            const res = await ctx.query(query)
            return res[0]
        } catch (err) {
            const error = new Error(`[Fetchq] failed to pick a document: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
}
