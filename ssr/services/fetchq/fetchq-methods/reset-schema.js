
const q = `
DROP SCHEMA IF EXISTS :schemaName CASCADE;
CREATE SCHEMA :schemaName;
`

export default ctx => async () => {
    try {
        await ctx.query(q.replace(/:schemaName/g, ctx.schema))
    } catch (err) {
        const error = new Error(`[Fetchq] failed to reset schema: ${ctx.schema} - ${err.message}`)
        error.original = err
        throw error
    }
}
