import { sqlSmallQuery } from '../lib/sql-small-query'

const q = `
DROP INDEX IF EXISTS
":schemaName_data".":queueName__idx__pk";
`

export default ctx => {
    const [ _q ] = sqlSmallQuery(ctx, q)

    return async (queueName) => {
        try {
            await ctx.query(_q
                .replace(/:queueName/g, queueName)
            )
        } catch (err) {
            const error = new Error(`[Fetchq] failed to drop indexes: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
    
}