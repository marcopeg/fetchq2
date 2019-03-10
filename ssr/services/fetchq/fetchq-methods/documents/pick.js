
import { FetchqPlan } from '../utils/plan'
import { sqlSmallQuery } from '../lib/sql-small-query'

const q = `
WITH
picked_tasks AS (
    UPDATE ":schemaName_data".":queueName__docs" SET 
        status = 2,
        attempts = attempts + 1,
        next_iteration = :lock
    WHERE subject IN (
        SELECT subject FROM ":schemaName_data".":queueName__docs"
        WHERE
            status = 1
        ORDER BY
            attempts ASC,
            next_iteration ASC
        LIMIT :limit
        FOR UPDATE FOR UPDATE SKIP LOCKED
    ) RETURNING subject
)
:queryStats
SELECT
    ':queueName' AS queue,
    docs.subject AS subject,
    docs.payload AS payload,
    docs.attempts AS attempts,
    docs.iterations AS iterations,
    docs.next_iteration AS nextIteration,
    docs.last_iteration AS lastIteration
FROM ":schemaName_data".":queueName__docs" AS docs WHERE subject IN (SELECT subject FROM picked_tasks)
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
