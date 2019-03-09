
const q1 = `
SELECT * FROM ":schemaName_data".":queueName__metrics";
`

export default ctx => async (queueName) => {
    try {
        const res = await ctx.query(q1
            .replace(/:schemaName/g, ctx.schema)
            .replace(/:queueName/g, queueName)
        )
        return res[0].reduce((acc, curr) => ({
            ...acc,
            [curr.metric]: {
                value: curr.amount,
                lastUpdate: curr.last_update,
            }
        }), {})
    } catch (err) {
        const error = new Error(`[Fetchq] failed to get metrics: ${queueName} - ${err.message}`)
        error.original = err
        throw error
    }
}
