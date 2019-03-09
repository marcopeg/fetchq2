import { START_FEATURE } from '@marcopeg/hooks'
import { getClient } from 'services/fetchq'
import { FEATURE_NAME } from './hooks'

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

            await client.docs.insert('tasks', [
                [ 'task1', client.utils.json, client.utils.now ],
                [ 'task2', client.utils.json, client.utils.plan('1y') ],
            ], { metrics })
            
            await client.docs.upsert('tasks', [
                [ 'task1', client.utils.json, client.utils.now ],
                [ 'task2', client.utils.json, client.utils.now ],
                [ 'task3', client.utils.json, client.utils.now ],
                [ 'task4', client.utils.json, client.utils.plan('1y') ],
            ], { metrics })

            const stats = await client.metrics.get('tasks')
            console.log(stats)
        },
    })
}
