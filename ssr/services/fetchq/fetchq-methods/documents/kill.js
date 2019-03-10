
import { sqlSubject } from '../lib/sql-subject'
import { sqlPayload } from '../lib/sql-payload'
import { sqlSmallQuery } from '../lib/sql-small-query'

const q = `
WITH
all_docs (subject, payload) AS (
    SELECT origin.*
    FROM ( VALUES :values ) AS origin (subject, payload)
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
        status = 4,
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
decrement_wip AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'wip', (SELECT COUNT(subject) FROM updated_docs), NOW()
    ON CONFLICT (metric) DO
    UPDATE SET amount = t.amount - EXCLUDED.amount, last_update = EXCLUDED.last_update
),
increment_kll AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'kll', (SELECT COUNT(subject) FROM updated_docs), NOW()
    ON CONFLICT (metric) DO
    UPDATE SET amount = t.amount + EXCLUDED.amount, last_update = EXCLUDED.last_update
)
`

const doc2str = (acc, doc) => {
    const subject = sqlSubject(doc.subject)
    const payload =  sqlPayload(doc.payload)
    return acc + `,(${subject},${payload})`
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
            return res[0]
        } catch (err) {
            const error = new Error(`[Fetchq] failed to kill documents: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
}
