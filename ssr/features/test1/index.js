import { START_FEATURE } from '@marcopeg/hooks'
import { getClient } from 'services/fetchq'
import { logInfo } from 'services/logger'
import { FEATURE_NAME } from './hooks'
import * as ingestUUID from './ingest-uuid'
import * as ingestCollide from './ingest-collide'

export const register = ({ registerAction, createHook }) => {
    registerAction({
        hook: START_FEATURE,
        name: FEATURE_NAME,
        trace: __filename,
        handler: async () => {
            const client = getClient()

            await client.resetSchema()
            await client.initSchema()
            await client.createQueue('tasks')

            await Promise.all([
                ingestUUID.start(1000, 100),
                ingestCollide.start(1000, 100),
            ])

            logInfo('')
            logInfo('---------')
            logInfo('> RESULTS')
            logInfo('---------')

            const uuid = ingestUUID.getState()
            const collide = ingestCollide.getState()
            logInfo(`[task1/uuid] ${uuid.poolSize} inserted documents in ${uuid.totalDuration}`)
            logInfo(`[task1/uuid] ${uuid.avgSpeed} docs/s`)
            logInfo(`[task1/uuid] ${uuid.avgSpeedStats.join(', ')}`)
            logInfo('------')
            logInfo(`[task1/collide] ${collide.poolSize} inserted documents in ${collide.totalDuration}`)
            logInfo(`[task1/collide] ${collide.avgSpeed} docs/s`)
            logInfo(`[task1/collide] ${collide.avgSpeedStats.join(', ')}`)
            logInfo('')
            const r1 = await client.query('SELECT COUNT(*) FROM fetchq.tasks')
            const r2 = await client.query('SELECT COUNT(*) FROM fetchq.tasks WHERE next_iteration < NOW()')
            const r3 = await client.query('SELECT COUNT(*) FROM fetchq.tasks WHERE next_iteration >= NOW()')
            logInfo(`--> Tot. documents: ${Number(r1[0][0].count) === uuid.poolSize + collide.poolSize}`)
            logInfo(`--> Tot. metrics:   ${Number(r1[0][0].count) === Number(r2[0][0].count) + Number(r3[0][0].count)}`)
            logInfo(`--> Tot. pending:   ${Number(r2[0][0].count)}`)
            logInfo(`--> Tot. scheduled: ${Number(r3[0][0].count)}`)
        },
    })
}
