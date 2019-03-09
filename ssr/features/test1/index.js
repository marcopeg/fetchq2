import { START_FEATURE } from '@marcopeg/hooks'
import { getClient } from 'services/fetchq'
import { logInfo } from 'services/logger'
import { FEATURE_NAME } from './hooks'
import * as ingestUUID from './ingest-uuid'
import * as ingestCollide from './ingest-collide'

const size = 1
const metrics = true

export const register = ({ registerAction, createHook }) => {
    registerAction({
        hook: START_FEATURE,
        name: FEATURE_NAME,
        trace: __filename,
        handler: async () => {
            const client = getClient()

            await client.resetSchema()
            await client.initSchema()
            await client.queue.create('tasks')

            if (size === 0) {
                await Promise.all([
                    ingestUUID.start(10000, 1000, { metrics }),
                    ingestCollide.start(10000, 1000, { metrics }),
                ])
            } else if (size === 1) {
                await Promise.all([
                    ingestUUID.start(100000 * 5, 10000, { metrics }),
                    ingestCollide.start(100000 * 5, 10000, { metrics }),
                ])
            } else if (size === 2) {
                await Promise.all([
                    ingestUUID.start(1000000 * 2, 25000, { metrics }),
                    ingestCollide.start(1000000 * 2, 25000, { metrics }),
                ])
            }

            logInfo('')
            logInfo('---------')
            logInfo('> RESULTS')
            logInfo('---------')

            const uuid = ingestUUID.getState()
            const collide = ingestCollide.getState()
            logInfo(`[task1/uuid] ${uuid.poolSize} inserted documents in ${uuid.totalDuration} at ${uuid.avgSpeed} docs/s`)
            logInfo(`[task1/uuid] ${uuid.avgSpeedStats.join(', ')}`)
            logInfo('------')
            logInfo(`[task1/collide] ${collide.poolSize} inserted documents in ${collide.totalDuration} at ${collide.avgSpeed} docs/s`)
            logInfo(`[task1/collide] ${collide.avgSpeedStats.join(', ')}`)
            logInfo('')

            const r1 = await client.query('SELECT COUNT(*) FROM fetchq_data.tasks__docs')
            const r2 = await client.query('SELECT COUNT(*) FROM fetchq_data.tasks__docs WHERE next_iteration < NOW()')
            const r3 = await client.query('SELECT COUNT(*) FROM fetchq_data.tasks__docs WHERE next_iteration >= NOW()')
            logInfo(`--> Tot. documents: ${Number(r1[0][0].count) === uuid.poolSize + collide.poolSize}`)
            logInfo(`--> Tot. metrics:   ${Number(r1[0][0].count) === Number(r2[0][0].count) + Number(r3[0][0].count)}`)
            
            try {
                const stats = await client.metrics.get('tasks')
                logInfo(`--> Tot. metrics:   ${Number(r1[0][0].count) === stats.cnt.value}`)
                logInfo(`--> Tot. pending:   ${Number(r2[0][0].count) === stats.pnd.value}`)
                logInfo(`--> Tot. planned:   ${Number(r3[0][0].count) === stats.pln.value}`)
                logInfo('')
                console.log(stats)
            } catch (err) {}
        },
    })
}
