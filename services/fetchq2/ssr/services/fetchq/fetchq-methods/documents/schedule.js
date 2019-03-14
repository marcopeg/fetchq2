
import { sqlSubject } from '../lib/sql-subject'
import { sqlPayload } from '../lib/sql-payload'
import { sqlNextIteration } from '../lib/sql-next-iteration'
import { sqlSmallQuery } from '../lib/sql-small-query'

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
        last_iteration = NOW(),
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
count_updated AS (
    SELECT COUNT(subject) AS amount FROM updated_docs
),
decrement_wip AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount - (SELECT MAX(amount) FROM count_updated),
    last_update = NOW()
    WHERE metric = 'wip'
),
increment_pln AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT COUNT(subject) FROM updated_docs WHERE status = 0),
    last_update = NOW()
    WHERE metric = 'pln'
),
increment_pnd AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT COUNT(subject) FROM updated_docs WHERE status = 1),
    last_update = NOW()
    WHERE metric = 'pnd'
),
increment_scd AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT MAX(amount) FROM count_updated),
    last_update = NOW()
    WHERE metric = 'scd'
)
`

const doc2str = (acc, doc) => {
    const subject = sqlSubject(doc.subject)
    const payload =  sqlPayload(doc.payload)
    const nextIteration = sqlNextIteration(doc.next_iteration)
    return acc + `,(${subject},${payload},${nextIteration})`
}

export default ctx => {
    const [ _q, _qStats ] = sqlSmallQuery(ctx, q, qStats)

    return async (queueName, docs, options = {}) => {
        try {
            const values = docs.reduce(doc2str, '').substr(1)
            const query = _q
                .replace(/:queueName/g, queueName)
                .replace(/:values/g, values)
                .replace(/:queryStats/g, options.metrics === false ? '' : (
                    _qStats.replace(/:queueName/g, queueName)
                ))
            
            const res = await ctx.query(query)
            return res.rows
        } catch (err) {
            const error = new Error(`[Fetchq] failed to schedule documents: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
}
