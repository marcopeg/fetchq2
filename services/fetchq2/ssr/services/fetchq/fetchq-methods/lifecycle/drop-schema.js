import { sqlSmallQuery } from '../lib/sql-small-query'

const q = `
DROP SCHEMA IF EXISTS :schemaName_data CASCADE;
DROP SCHEMA IF EXISTS :schemaName_catalog CASCADE;
`

export default ctx => {
    const [ _q ] = sqlSmallQuery(ctx, q)

    return async () => {
        try {
            await ctx.query(_q)
        } catch (err) {
            const error = new Error(`[Fetchq] failed to drop fetchq schema: ${ctx.schema} - ${err.message}`)
            error.original = err
            throw error
        }
    }
    
}