import expect from 'expect'
import { test } from 'lib/tester'
import { START_FEATURE } from '@marcopeg/hooks'
import { getClient } from 'services/fetchq'
import { logInfo } from 'services/logger'
import { FEATURE_NAME } from './hooks'
import { bulkInsert, logReport } from './bulk-insert'

const testSize = 0
const testType = 'process'

const processResetSchema = false
const processBulkInsert = true
const processUseMetrics = true
const processSize = 1000

export const register = ({ registerAction, createHook }) => {
    registerAction({
        hook: START_FEATURE,
        name: FEATURE_NAME,
        trace: __filename,
        handler: async () => {
            const client = getClient()
            let poolSize= 0
            let batchSize = 0

            switch (testSize) {
                case 0:
                    poolSize = 10000
                    batchSize = 1000
                    break
                case 1:
                    poolSize = 500000
                    batchSize = 10000
                    break
                case 2:
                    poolSize = 1000000 * 5
                    batchSize = 25000
                    break
            }

            let report1
            let report2
            switch (testType) {
                case 'insert-metrics':
                    logInfo('>>>>>> Test 1 <<<<<<<')
                    report1 = await bulkInsert({
                        poolSize,
                        batchSize,
                        resetSchema: true,
                    })
        
                    logInfo('>>>>>> Test 2 <<<<<<<')
                    report2 = await bulkInsert({
                        poolSize,
                        batchSize,
                        useMetrics: false,
                        resetSchema: true,
                    })
        
                    logReport(`Insert${testSize} - with metrics`, report1)
                    logReport(`Insert${testSize} - no metrics`, report2)
                    break
                
                case 'insert-indexes':
                    logInfo('>>>>>> Test 1 <<<<<<<')
                    report1 = await bulkInsert({
                        poolSize,
                        batchSize,
                        resetSchema: true,
                        useIndex: true,
                        useMetrics: false,
                    })
        
                    logInfo('>>>>>> Test 2 <<<<<<<')
                    report2 = await bulkInsert({
                        poolSize,
                        batchSize,
                        resetSchema: true,
                        useIndex: false,
                        useMetrics: false,
                    })
        
                    logReport(`Insert${testSize} - with indexes`, report1)
                    logReport(`Insert${testSize} - no indexes`, report2)
                    break

                case 'insert-copy':
                    logInfo('>>>>>> Test 1 <<<<<<<')
                    report1 = await bulkInsert({
                        poolSize,
                        batchSize,
                        resetSchema: true,
                        useIndex: true,
                        useMetrics: true,
                    })
        
                    logInfo('>>>>>> Test 2 <<<<<<<')
                    report2 = await bulkInsert({
                        poolSize,
                        batchSize,
                        resetSchema: true,
                        useIndex: false,
                        useMetrics: false,
                    })
        
                    logReport(`Insert${testSize} - with indexes`, report1)
                    logReport(`Insert${testSize} - no indexes`, report2)
                    break
                
                case 'process':
                    logInfo('>>>>>> Pick Test <<<<<<<')
                    if (processBulkInsert) {
                        report1 = await bulkInsert({
                            poolSize,
                            batchSize,
                            resetSchema: processResetSchema,
                            useMetrics: processUseMetrics,
                        })
                        logReport('pick', report1)
                    }

                    let start
                    let docs
                    let lapsed
                    let speed

                    // pick
                    start = new Date()
                    docs = await client.docs.pick('tasks', processSize)
                    lapsed = new Date() - start
                    speed = Math.floor(docs.length * 1000 / lapsed)
                    logInfo(`[pick] ${docs.length}/${processSize} in ${lapsed}ms - ${speed} docs/s`)

                    // schedule
                    start = new Date()
                    await client.docs.schedule('tasks', docs.map(doc => ({
                        ...doc,
                        next_iteration: client.utils.plan('1y'),
                    })))
                    lapsed = new Date() - start
                    speed = Math.floor(docs.length * 1000 / lapsed)
                    logInfo(`[schedule] ${docs.length} in ${new Date() - start}ms - ${speed} docs/s`)

                    // complete

                    // kill

                    // ?? log error ??

                    break
            }

            

            /*
            let stats
            const client = getClient()

            await client.resetSchema()
            await client.initSchema()
            await client.queue.create('tasks')

            let poolSize = 0
            let batchSize = 0

            switch (size) {
                case 0:
                    poolSize = 10000
                    batchSize = 1000
                    break
                case 1:
                    poolSize = 100000 * 5
                    batchSize = 10000
                    break
                case 2:
                    poolSize = 1000000 * 5
                    batchSize = 25000
                    break
            }

            const start = new Date()
            await Promise.all([
                ingestUUID.start(poolSize, batchSize, { metrics }),
                ingestCollide.start(poolSize, batchSize, { metrics }),
                upsertUUID.start(poolSize, batchSize, { metrics }),
                upsertCollide.start(poolSize, batchSize, { metrics }),
            ])

            logInfo(`[task1/bulkInsert] total time: ${new Date() - start}ms`)

            // Show insert performances
            const uuid = ingestUUID.getState()
            const collide = ingestCollide.getState()
            const uuid1 = upsertUUID.getState()
            const collide1 = upsertCollide.getState()
            logInfo(`[task1/insert(uuid)] ${uuid.poolSize} inserted documents in ${uuid.totalDuration} at ${uuid.avgSpeed} docs/s`)
            logInfo(`[task1/insert(uuid)] ${uuid.avgSpeedStats.join(', ')}`)
            logInfo('------')
            logInfo(`[task1/insert(collide)] ${collide.poolSize} inserted documents in ${collide.totalDuration} at ${collide.avgSpeed} docs/s`)
            logInfo(`[task1/insert(collide)] ${collide.avgSpeedStats.join(', ')}`)
            logInfo('------')
            logInfo(`[task1/upsert(uuid)] ${uuid1.poolSize} upserted documents (uuid) in ${uuid1.totalDuration} at ${uuid1.avgSpeed} docs/s`)
            logInfo(`[task1/upsert(uuid)] ${uuid1.avgSpeedStats.join(', ')}`)
            logInfo('------')
            logInfo(`[task1/upsert(collide)] ${collide1.poolSize} upserted documents (random) in ${collide1.totalDuration} at ${collide1.avgSpeed} docs/s`)
            logInfo(`[task1/upsert(collide)] ${collide1.avgSpeedStats.join(', ')}`)
            logInfo('')


            // Test basic behaviour
            if (metrics) {
                stats = await client.metrics.get('tasks')
                test('Min docs in the queue', () => expect(stats.cnt.value).toBeGreaterThanOrEqual(poolSize * 4 * 0.9))
                test('Some docs should be updated', () => expect(stats.upd.value).toBeGreaterThan(0))
                test('Some docs should be planned', () => expect(stats.pln.value).toBeGreaterThan(poolSize / 3))
                test('Some docs should be pending', () => expect(stats.pnd.value).toBeGreaterThan(poolSize / 3))
            }

            // logInfo('')
            // logInfo('---------')
            // logInfo('> RESULTS')
            // logInfo('---------')

            

            // const r1 = await client.query('SELECT COUNT(*) FROM fetchq_data.tasks__docs')
            // const r2 = await client.query('SELECT COUNT(*) FROM fetchq_data.tasks__docs WHERE next_iteration < NOW()')
            // const r3 = await client.query('SELECT COUNT(*) FROM fetchq_data.tasks__docs WHERE next_iteration >= NOW()')
            // logInfo(`--> Tot. documents: ${Number(r1[0][0].count) === uuid.poolSize + collide.poolSize}`)
            // logInfo(`--> Tot. metrics:   ${Number(r1[0][0].count) === Number(r2[0][0].count) + Number(r3[0][0].count)}`)
            
            // try {
            //     const stats = await client.metrics.get('tasks')
            //     logInfo(`--> Tot. metrics:   ${Number(r1[0][0].count) === stats.cnt.value}`)
            //     logInfo(`--> Tot. pending:   ${Number(r2[0][0].count) === stats.pnd.value}`)
            //     logInfo(`--> Tot. planned:   ${Number(r3[0][0].count) === stats.pln.value} - ${Number(r3[0][0].count)}/${stats.pln.value}`)
            //     logInfo('')
            //     console.log(stats)
            // } catch (err) {}
            */
        },
    })
}
