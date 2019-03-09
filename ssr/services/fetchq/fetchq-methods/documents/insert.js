
import { sqlSubject } from '../lib/sql-subject'
import { sqlPayload } from '../lib/sql-payload'
import { sqlNextIteration } from '../lib/sql-next-iteration'

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
increment_cnt AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'cnt', (
        SELECT COUNT(subject) FROM inserted_docs
    ), NOW()
    ON CONFLICT (metric) DO UPDATE SET 
    amount = t.amount + EXCLUDED.amount,
    last_update = EXCLUDED.last_update
),
increment_ent AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'ent', (
        SELECT COUNT(subject) FROM inserted_docs
    ), NOW()
    ON CONFLICT (metric) DO UPDATE SET 
    amount = t.amount + EXCLUDED.amount,
    last_update = EXCLUDED.last_update
),
increment_pln AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'pln', (
        SELECT COUNT(subject) FROM inserted_docs
        WHERE status = 0
    ), NOW()
    ON CONFLICT (metric) DO UPDATE SET 
    amount = t.amount + EXCLUDED.amount,
    last_update = EXCLUDED.last_update
),
increment_pnd AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'pnd', (
        SELECT COUNT(subject) FROM inserted_docs
        WHERE status = 1
    ), NOW()
    ON CONFLICT (metric) DO UPDATE SET
    amount = t.amount + EXCLUDED.amount,
    last_update = EXCLUDED.last_update
)
`

const doc2str = (acc, doc) => {
    const subject = sqlSubject(doc[0])
    const payload =  sqlPayload(doc[1])
    const nextIteration = sqlNextIteration(doc[2])
    return acc + `,(${subject},${payload},${nextIteration})`
}

export default ctx => async (queueName, docs, options = {}) => {
    try {
        const values = docs.reduce(doc2str, '').substr(1)
        const query = q
            .replace(/:schemaName/g, ctx.schema)
            .replace(/:queueName/g, queueName)
            .replace(/:values/g, values)
            .replace(/:queryStats/g, options.metrics === false ? '' : (
                qStats
                    .replace(/:schemaName/g, ctx.schema)
                    .replace(/:queueName/g, queueName)
            ))
        
        const res = await ctx.query(query)

        return res[0]
    } catch (err) {
        const error = new Error(`[Fetchq] failed to insert documents: ${queueName} - ${err.message}`)
        error.original = err
        throw error
    }
}
