
import { FetchqPlan } from '../utils/plan'
import { sqlSubject } from '../lib/sql-subject'
import { sqlPayload } from '../lib/sql-payload'
import { sqlNextIteration } from '../lib/sql-next-iteration'
import { sqlSmallQuery } from '../lib/sql-small-query'

// completed_tasks AS (
//     CASE WHEN origin.next_iteration <= NOW() THEN 1
//     ELSE 0
// END AS status 
// attempts = 0,
// iterations = iterations + 1,
// next_iteration = :plan
//         SELECT subject FROM ":schemaName_data".":queueName__docs"
//         WHERE subject = :subject
//         LIMIT 1
//         FOR UPDATE FOR UPDATE SKIP LOCKED
//     ) RETURNING *
// ),

const q = `
WITH
all_docs (subject, payload, next_iteration, status) AS (
	SELECT
		origin.*, 
		CASE WHEN origin.next_iteration <= NOW() THEN 1
             ELSE 0
        END AS status 
    FROM ( VALUES :values ) AS origin (subject, payload, next_iteration)
    -- safety check on the incoming task
    JOIN ":schemaName_data".":queueName__docs" AS docs ON origin.subject = docs.subject
    WHERE docs.status = 2
    FOR UPDATE FOR UPDATE SKIP LOCKED
),
updated_docs AS (
    UPDATE ":schemaName_data".":queueName__docs" AS target
    SET 
        attempts = 0,
        iterations = iterations + 1,
        status = origin.status,
        next_iteration = origin.next_iteration,
        payload = origin.payload::jsonb
    FROM all_docs AS origin
    WHERE origin.subject = target.subject
    RETURNING target.*
)
:queryStats
SELECT * FROM updated_docs
`

const qStats = `
,
decrement_wip AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'wip', (SELECT COUNT(subject) FROM updated_docs), NOW()
    ON CONFLICT (metric) DO
    UPDATE SET amount = t.amount - EXCLUDED.amount, last_update = EXCLUDED.last_update
),
increment_pln AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'pln', (SELECT COUNT(subject) FROM updated_docs WHERE status = 0), NOW()
    ON CONFLICT (metric) DO
    UPDATE SET amount = t.amount + EXCLUDED.amount, last_update = EXCLUDED.last_update
),
increment_pnd AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'pnd', (SELECT COUNT(subject) FROM updated_docs WHERE status = 1), NOW()
    ON CONFLICT (metric) DO
    UPDATE SET amount = t.amount + EXCLUDED.amount, last_update = EXCLUDED.last_update
),
increment_scd AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'scd', (SELECT COUNT(subject) FROM updated_docs), NOW()
    ON CONFLICT (metric) DO
    UPDATE SET amount = t.amount + EXCLUDED.amount, last_update = EXCLUDED.last_update
)
`

const doc2str = (acc, doc) => {
    const subject = sqlSubject(doc.subject)
    const payload =  sqlPayload(doc.payload)
    const nextIteration = sqlNextIteration(doc.nextIteration)
    return acc + `,(${subject},${payload},${nextIteration})`
}

export default ctx => {
    const [ _q, _qStats ] = sqlSmallQuery(ctx, q, qStats, true)

    return async (queueName, docs, options = {}) => {
        try {
            console.log(docs)
            const values = docs.reduce(doc2str, '').substr(1)
            const query = _q
                .replace(/:queueName/g, queueName)
                .replace(/:values/g, values)
                // .replace(/:subject/g, sqlSubject(doc.subject))
                // .replace(/:plan/g, plan.toString())
                .replace(/:queryStats/g, options.metrics === false ? '' : (
                    _qStats.replace(/:queueName/g, queueName)
                ))
            
            console.log(query)
            const res = await ctx.query(query)
            return res[0]
        } catch (err) {
            const error = new Error(`[Fetchq] failed to schedule documents: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
}
