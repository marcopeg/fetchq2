
const q = `
CREATE TABLE ":schemaName_data".":queueName__docs" (
    subject character varying(50) PRIMARY KEY,
    payload jsonb DEFAULT '{}',
    attempts integer DEFAULT 0,
    iterations integer DEFAULT 0,
    next_iteration timestamp with time zone,
    last_iteration timestamp with time zone
);
CREATE TABLE ":schemaName_data".":queueName__metrics" (
    metric character varying(50) PRIMARY KEY,
    amount integer DEFAULT 0,
    last_update timestamp with time zone
);
`

export default ctx => async (queueName) => {
    try {
        await ctx.query(q
            .replace(/:schemaName/g, ctx.schema)
            .replace(/:queueName/g, queueName)
        )
    } catch (err) {
        if (!err.original || err.original.code !== '42P07') {
            const error = new Error(`[Fetchq] failed to create queue: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
}
