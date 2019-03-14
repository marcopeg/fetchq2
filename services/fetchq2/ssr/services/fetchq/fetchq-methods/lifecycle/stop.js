/**
 * - retrieve queues settings and keep them up-to-date via pubsub
 */


import { logError, logDebug } from '@marcopeg/utils/lib/logger'


export default ctx => {

    // try/catch with auto schema init capability
    return async () => {
        try {
            await ctx.conn.pubsub.close()
            await ctx.conn.pool.end()
        } catch (err) {
            const error = new Error(`[Fetchq] failed to tear down: ${err.message}`)
            error.original = err
            throw error
        }
    }    
}