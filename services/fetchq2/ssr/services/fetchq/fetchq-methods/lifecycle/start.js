/**
 * - retrieve queues settings and keep them up-to-date via pubsub
 */

import { sqlSmallQuery } from '../lib/sql-small-query'
import { logError, logDebug } from '@marcopeg/utils/lib/logger'

const q1 = `
SELECT * FROM ":schemaName_catalog"."fq_queues";
`

export default ctx => {
    const [ _q1 ] = sqlSmallQuery(ctx, q1)

    const startHandler = async () => {
        try {
            const res = await ctx.query(_q1)
            ctx.queues = res.rows.reduce((acc, curr) => ({
                ...acc,
                [curr.subject]: curr,
            }), {})

            // NOTE: it tries to create a new date instace as kinda of
            // a dynamic test that will check if the payload that we receive
            // is valid json from the settings table
            ctx.subscribe('fetchq_settings', (payload) => {
                try {
                    new Date(payload.created_at)
                    ctx.queues[payload.subject] = payload
                } catch (err) {
                    logError(`[fetchq] queues settings trigger: ${err.msg}`)
                    logDebug(err)
                }
            })
            
            return ctx.isReady = true
        } catch (err) {
            if (err.code === '42P01') {
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