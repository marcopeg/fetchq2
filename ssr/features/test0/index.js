import expect from 'expect'
import { test } from 'lib/test'
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
            let stats
            const client = getClient()

            await client.resetSchema()
            await client.initSchema()
            await client.queue.create('tasks')

            await client.docs.insert('tasks', [
                [ 'task1', client.utils.json, client.utils.now ],
                [ 'task2', client.utils.json, client.utils.plan('1y') ],
            ], { metrics })

            stats = await client.metrics.get('tasks')
            test('Should add 2 docs', () => expect(stats.ent.value).toBe(2))
            
            await client.docs.upsert('tasks', [
                [ 'task1', client.utils.json, client.utils.now ],
                [ 'task2', client.utils.json, client.utils.now ],
                [ 'task3', client.utils.json, client.utils.now ],
                [ 'task4', client.utils.json, client.utils.plan('1y') ],
            ], { metrics })

            stats = await client.metrics.get('tasks')
            test('There should be 4 docs', () => expect(stats.cnt.value).toBe(4))
            test('Should should have updated 2 docs', () => expect(stats.upd.value).toBe(2))

            const docs = await client.docs.pick('tasks')
            await client.docs.schedule('tasks', docs.map(doc => ({
                ...doc,
                nextIteration: client.utils.plan('100 y'),
                payload: { done: true },
            })))

            stats = await client.metrics.get('tasks')
            test('Should have piked 1 doc', () => expect(stats.pkd.value).toBe(1))
            test('Should have rescheduled 1 doc', () => expect(stats.scd.value).toBe(1))
        },
    })
}
