
const q1 = `
CREATE SCHEMA IF NOT EXISTS :schemaName;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
`

export default ctx => async () => {
    try {
        await ctx.query(q1.replace(/:schemaName/g, ctx.schema))
    } catch (err) {
        const error = new Error(`[Fetchq] failed to init schema: ${ctx.schema} - ${err.message}`)
        error.original = err
        throw error
    }
}
