import { START_FEATURE } from '@marcopeg/hooks'
import { getClient } from 'services/fetchq'
import { FEATURE_NAME } from './hooks'

export const register = ({ registerAction }) => {
    registerAction({
        hook: START_FEATURE,
        name: FEATURE_NAME,
        trace: __filename,
        handler: async (settings) => {
            if ('test2' !== settings.feature) return

            const start = new Date()
            const client = getClient()

            await client.start()
            await client.queue.create('tasks')

            for (let i = 0; i < 10; i++) {
                console.log('step ', i)

                const unique_values = Array(25000).fill(0).map(_ => ([
                    client.utils.uuid,
                    client.utils.payload,
                    Math.random() > 0.5 ? client.utils.plan('1y') : client.utils.now,
                ]))

                const mixed_values = Array(25000).fill(0).map(_ => ([
                    `task-${Math.floor(Math.random() * Math.floor(100000))}`,
                    client.utils.payload,
                    Math.random() > 0.5 ? client.utils.plan('1y') : client.utils.now,
                ]))

                const upsert_mixed = Array(25000).fill(0).map(_ => ([
                    `task-${Math.floor(Math.random() * Math.floor(100000))}`,
                    client.utils.payload,
                    Math.random() > 0.5 ? client.utils.plan('1y') : client.utils.now,
                ]))

                const pick1 = await client.docs.pick('tasks', 1000)
                const pick2 = await client.docs.pick('tasks', 1000)

                await Promise.all([
                    client.docs.insert('tasks', unique_values),
                    client.docs.insert('tasks', mixed_values),
                    client.docs.upsert('tasks', upsert_mixed),
                    client.docs.schedule('tasks', pick1.map(doc => ({
                        ...doc,
                        next_iteration: client.utils.plan('1y'),
                    }))),
                    client.docs.complete('tasks', pick2),
                ])

            }

            const duration = new Date() - start
            console.log('duration', duration)
        },
    })
}
