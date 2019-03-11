import { sqlSmallQuery } from '../lib/sql-small-query'

const q1 = `
WITH
settings AS (
    SELECT * FROM ":schemaName_catalog"."fq_queues"
)
SELECT * FROM settings;
`

export default ctx => {
    const [ _q1 ] = sqlSmallQuery(ctx, q1)

    const startHandler = async () => {
        try {
            const res = await ctx.query(_q1)
            ctx.queues = res[0].reduce((acc, curr) => ({
                ...acc,
                [curr.subject]: curr,
            }), {})
            
            console.log(ctx.queues)
            return ctx.isReady = true
        } catch (err) {
            if (err.original && err.original.code === '42P01') {
                return false
            }
            throw err
        }
    }

    // try/catch with auto schema init capability
    return async () => {
        try {
            // try to start right away
            const res = await startHandler()
            if (res === true) {
                return res
            }

            // fallback on init the schema
            await ctx.initSchema()
            return await startHandler()
        } catch (err) {
            const error = new Error(`[Fetchq] failed to start fetchq: ${ctx.schema} - ${err.message}`)
            error.original = err
            throw error
        }
    }    
}