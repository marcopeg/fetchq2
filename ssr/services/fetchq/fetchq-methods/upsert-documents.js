
import { sqlPayload } from './lib/sql-payload'
import { sqlNextIteration } from './lib/sql-next-iteration'

const q = `
WITH
all_docs (subject, payload, next_iteration) AS (
    VALUES :values
),
inserted_docs AS (
    INSERT INTO ":schemaName".":queueName" (subject, payload, next_iteration)
    SELECT subject, payload::jsonb, next_iteration FROM all_docs
    ON CONFLICT (subject) DO NOTHING
    RETURNING *
),
skipped_docs AS (
    SELECT
        _source.*, 
        _values.payload AS payload_new,
        _values.next_iteration AS next_iteration_new,
        _source.next_iteration AS next_iteration_old
    FROM ":schemaName".":queueName" as _source
    JOIN all_docs AS _values ON _source.subject = _values.subject
    WHERE _source.subject NOT IN (SELECT subject FROM inserted_docs)
    FOR UPDATE FOR UPDATE SKIP LOCKED
),
updated_docs AS (
    UPDATE ":schemaName".":queueName" AS docs
    SET
        payload = _value.payload_new::jsonb,
        next_iteration = _value.next_iteration_new
    FROM (
        SELECT * FROM skipped_docs
    ) AS _value
    WHERE docs.subject = _value.subject
    RETURNING _value.*
),
results AS (
    SELECT
        'updated' AS action,
        t1.subject,
        t1.payload,
        t1.attempts,
        t1.iterations,
        t1.next_iteration,
        t1.last_iteration
    FROM updated_docs AS t1
    UNION ALL
    SELECT
        'created' AS action,
        t2.subject,
        t2.payload,
        t2.attempts,
        t2.iterations,
        t2.next_iteration,
        t2.last_iteration
    FROM inserted_docs AS t2
)
SELECT * FROM results
`

const doc2str = (acc, doc) => {
    const subject = doc[0]
    const payload =  sqlPayload(doc[1])
    const nextIteration = sqlNextIteration(doc[2])
    return acc + `,('${subject}','${payload}',${nextIteration})`
}

export default ctx => async (queueName, docs) => {
    const values = docs.reduce(doc2str, '').substr(1)

    try {
        const res = await ctx.query(q
            .replace(/:schemaName/g, ctx.schema)
            .replace(/:queueName/g, queueName)
            .replace(/:values/g, values)
        )

        return res[0]
    } catch (err) {
        const error = new Error(`[Fetchq] failed to insert documents: ${queueName} - ${err.message}`)
        error.original = err
        throw error
    }
}
