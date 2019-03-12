import { sqlSmallQuery } from '../lib/sql-small-query'

const q1 = `
WITH
cnt AS (
    SELECT 1 AS id, COUNT(*) AS cnt FROM ":schemaName_data".":queueName__docs"
),
pnd AS (
    SELECT 1 AS id, COUNT(*) AS pnd FROM ":schemaName_data".":queueName__docs"
    WHERE status = 1
),
pln AS (
    SELECT 1 AS id, COUNT(*) AS pln FROM ":schemaName_data".":queueName__docs"
    WHERE status = 0
),
wip AS (
    SELECT 1 AS id, COUNT(*) AS wip FROM ":schemaName_data".":queueName__docs"
    WHERE status = 2
),
cpl AS (
    SELECT 1 AS id, COUNT(*) AS cpl FROM ":schemaName_data".":queueName__docs"
    WHERE status = 3
),
kll AS (
    SELECT 1 AS id, COUNT(*) AS kll FROM ":schemaName_data".":queueName__docs"
    WHERE status = 4
)

SELECT 
    cnt.cnt::integer,
    pnd.pnd::integer,
    pln.pln::integer,
    wip.wip::integer,
    cpl.cpl::integer,
    kll.kll::integer,
    NOW() AS last_update
FROM cnt AS cnt
JOIN pnd AS pnd ON cnt.id = pnd.id
JOIN pln AS pln ON cnt.id = pln.id
JOIN wip AS wip ON cnt.id = wip.id
JOIN cpl AS cpl ON cnt.id = cpl.id
JOIN kll AS kll ON cnt.id = kll.id
;
`

export default ctx => {
    const [ _q1 ] = sqlSmallQuery(ctx, q1)

    return async (queueName) => {
        try {
            const query = _q1.replace(/:queueName/g, queueName)
            const res = await ctx.query(query)
            return {
                cnt: {
                    value: res.rows[0].cnt,
                    last_update: res.rows[0].last_update,
                },
                pln: {
                    value: res.rows[0].pln,
                    last_update: res.rows[0].last_update,
                },
                pnd: {
                    value: res.rows[0].pnd,
                    last_update: res.rows[0].last_update,
                },
                wip: {
                    value: res.rows[0].wip,
                    last_update: res.rows[0].last_update,
                },
                cpl: {
                    value: res.rows[0].cpl,
                    last_update: res.rows[0].last_update,
                },
                kll: {
                    value: res.rows[0].kll,
                    last_update: res.rows[0].last_update,
                }
            }
        } catch (err) {
            const error = new Error(`[Fetchq] failed to compute metrics: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }
}
