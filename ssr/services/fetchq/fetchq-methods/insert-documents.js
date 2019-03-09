
import { sqlSubject } from './lib/sql-subject'
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
)
SELECT * FROM inserted_docs
`

const doc2str = (acc, doc) => {
    const subject = sqlSubject(doc[0])
    const payload =  sqlPayload(doc[1])
    const nextIteration = sqlNextIteration(doc[2])
    return acc + `,(${subject},${payload},${nextIteration})`
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
