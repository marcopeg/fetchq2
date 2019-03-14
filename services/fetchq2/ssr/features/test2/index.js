import { START_FEATURE } from '@marcopeg/hooks'
import { getClient } from 'services/fetchq'
import { FEATURE_NAME } from './hooks'
import { logError, logInfo } from 'services/logger'

export const register = ({ registerAction }) => {
    registerAction({
        hook: START_FEATURE,
        name: FEATURE_NAME,
        trace: __filename,
        handler: async (settings) => {
            if ('test2' !== settings.feature) return

            const start = new Date()
            const client = getClient()

            const iterations = 100
            const batchSize = 500
            const pickSize = 10
            const variance = 10000

            await client.start()
            await client.queue.create('tasks')

            for (let i = 0; i < iterations; i++) {
                logInfo(`[test2] step ${i}/${iterations}`)

                const unique_values = Array(batchSize).fill(0).map(_ => ([
                    client.utils.uuid,
                    client.utils.payload,
                    Math.random() > 0.5 ? client.utils.plan('1y') : client.utils.now,
                ]))

                const mixed_values = Array(batchSize).fill(0).map(_ => ([
                    `task-${Math.floor(Math.random() * Math.floor(variance))}`,
                    client.utils.payload,
                    Math.random() > 0.5 ? client.utils.plan('1y') : client.utils.now,
                ]))

                const upsert_mixed = Array(1).fill(0).map(_ => ([
                    `task-${Math.floor(Math.random() * Math.floor(variance))}`,
                    client.utils.payload,
                    Math.random() > 0.5 ? client.utils.plan('1y') : client.utils.now,
                ]))
                
                await Promise.all([
                    new Promise(async (resolve) => {
                        try {
                            await client.docs.insert('tasks', unique_values)
                        } catch (err) {
                            logError(`[test2] insert unique_values - ${err.message}`)
                        } finally {
                            resolve()
                        }
                    }),
                    new Promise(async (resolve) => {
                        try {
                            await client.docs.insert('tasks', mixed_values)
                        } catch (err) {
                            logError(`[test2] insert mixed_values - ${err.message}`)
                        } finally {
                            resolve()
                        }
                    }),
                    new Promise(async (resolve) => {
                        try {
                            await client.docs.upsert('tasks', upsert_mixed)
                        } catch (err) {
                            logError(`[test2] insert upsert_mixed - ${err.message}`)
                        } finally {
                            resolve()
                        }
                    }),
                    new Promise(async (resolve) => {
                        try {
                            const pick1 = await client.docs.pick('tasks', pickSize)
                            if (!pick1.length) return resolve()
                            await client.docs.schedule('tasks', pick1.map(doc => ({
                                ...doc,
                                next_iteration: client.utils.plan('1y'),
                            })))
                        } catch (err) {
                            logError(`[test2] pick/schedule - ${err.message}`)
                        } finally {
                            resolve()
                        }
                    }),
                    new Promise(async (resolve) => {
                        try {
                            const pick2 = await client.docs.pick('tasks', pickSize)
                            if (!pick2.length) return resolve()
                            await client.docs.complete('tasks', pick2)
                        } catch (err) {
                            logError(`[test2] pick/complete - ${err.message}`)
                        } finally {
                            resolve()
                        }
                    }),
                ])

            }

            const duration = new Date() - start
            console.log('duration', duration)
        },
    })
}
