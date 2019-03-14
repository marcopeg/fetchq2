
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
count_picked AS (
    SELECT COUNT(subject) AS amount FROM picked_tasks
),
increment_wip AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT MAX(amount) FROM count_picked),
    last_update = NOW()
    WHERE metric = 'wip'
),
decrement_pnd AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount - (SELECT MAX(amount) FROM count_picked),
    last_update = NOW()
    WHERE metric = 'pnd'
),
increment_pkd AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT MAX(amount) FROM count_picked),
    last_update = NOW()
    WHERE metric = 'pkd'
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
            return res.rows
        } catch (err) {
            const error = new Error(`[Fetchq] failed to pick a document: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
}
