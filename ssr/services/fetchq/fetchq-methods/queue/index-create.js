import { sqlSmallQuery } from '../lib/sql-small-query'

const q = `
CREATE INDEX IF NOT EXISTS
:queueName__idx__pk ON ":schemaName_data".":queueName__docs"
(attempts int4_ops, next_iteration timestamptz_ops)
WHERE status = 1;
`

export default ctx => {
    const [ _q ] = sqlSmallQuery(ctx, q)

    return async (queueName) => {
        try {
            await ctx.query(_q
                .replace(/:queueName/g, queueName)
            )
        } catch (err) {
            const error = new Error(`[Fetchq] failed to create indexes: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
}
