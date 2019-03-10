import { sqlSmallQuery } from '../lib/sql-small-query'

const q = `
WITH
settings ( max_attempts ) AS (
    VALUES ( 5 )
),
orphan_docs AS (
    SELECT settings.max_attempts, docs.* FROM settings
    LEFT JOIN ":schemaName_data".":queueName__docs" AS docs
    ON 1 = 1
    WHERE docs.status = 2
    AND docs.attempts < settings.max_attempts
    AND docs.next_iteration < NOW()
),
updated_docs AS (
   UPDATE ":schemaName_data".":queueName__docs" AS target
   SET
       status = 1
   FROM (
       SELECT * FROM orphan_docs
   ) AS origin
   WHERE origin.subject = target.subject
   RETURNING 
       target
)
:queryStats
SELECT * FROM orphan_docs;
`

const qStats = `
,
increment_pnd AS (
    INSERT INTO ":schemaName_data".":queueName__metrics" AS t (metric, amount, last_update)
    SELECT 'pnd', (
        SELECT COUNT(subject) FROM orphan_docs
    ), NOW()
    ON CONFLICT (metric) DO
    UPDATE SET
    amount = t.amount + EXCLUDED.amount,
    last_update = EXCLUDED.last_update
)`

export default ctx => {
    const [ _q, _qStats ] = sqlSmallQuery(ctx, q, qStats)

    return async (queueName, options = {}) => {
        try {
            const res = await ctx.query(_q
                .replace(/:queueName/g, queueName)
                .replace(/:queryStats/g, options.metrics === false ? '' : (
                    _qStats.replace(/:queueName/g, queueName)
                ))
            )
            console.log(res[0])
        } catch (err) {
            const error = new Error(`[Fetchq] failed to reschedule orphans: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
    
}