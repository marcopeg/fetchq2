import { sqlSmallQuery } from '../lib/sql-small-query'

const q1 = `
SELECT * FROM ":schemaName_data".":queueName__metrics";
`

export default ctx => {
    const [ _q1 ] = sqlSmallQuery(ctx, q1)

    return async (queueName) => {
        try {
            const query = _q1.replace(/:queueName/g, queueName)
            const res = await ctx.query(query)
            return res.rows.reduce((acc, curr) => ({
                ...acc,
                [curr.metric]: {
                    value: curr.amount,
                    last_update: curr.last_update,
                }
            }), {})
        } catch (err) {
            const error = new Error(`[Fetchq] failed to get metrics: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
}
