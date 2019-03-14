
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
),
inserted_docs AS (
    INSERT INTO ":schemaName_data".":queueName__docs" (subject, payload, next_iteration, status)
    SELECT subject, payload::jsonb, next_iteration, status FROM all_docs
    ON CONFLICT (subject) DO NOTHING
    RETURNING *
)
:queryStats
SELECT * FROM inserted_docs
`

const qStats = `
,
count_inserted AS (
    SELECT COUNT(subject) AS amount FROM inserted_docs
),
increment_cnt AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT MAX(amount) FROM count_inserted),
    last_update = NOW()
    WHERE metric = 'cnt'
),
increment_ent AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT MAX(amount) FROM count_inserted),
    last_update = NOW()
    WHERE metric = 'ent'
),
increment_pln AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT COUNT(subject) FROM inserted_docs WHERE status = 0),
    last_update = NOW()
    WHERE metric = 'pln'
),
increment_pnd AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT COUNT(subject) FROM inserted_docs WHERE status = 1),
    last_update = NOW()
    WHERE metric = 'pnd'
)
`

const doc2str = (acc, doc) => {
    const subject = sqlSubject(doc[0])
    const payload =  sqlPayload(doc[1])
    const nextIteration = sqlNextIteration(doc[2])
    return acc + `,(${subject},${payload},${nextIteration})`
}

export default ctx => {
    const [ _q, _qStats ] = sqlSmallQuery(ctx, q, qStats)

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
            const error = new Error(`[Fetchq] failed to insert documents: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
}
