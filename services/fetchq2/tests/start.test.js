import pause from '@marcopeg/utils/lib/pause'
import { getClient } from './get-client'


describe('Fetchq Start', () => {
    let client
    beforeAll(async () => {
        client = await getClient()
    })

    beforeEach(async () => {
        await client.dropSchema()
    })

    test('Fetchq should not ready by default', async () => {
        expect(await client.isReady).toBe(false)
    })

    test('Fetchq should self-init the schema if needed', async () => {
        const res = await client.start()
        expect(res).toBe(true)
        expect(await client.isReady).toBe(true)
    })

    test('Fetchq should read the list of available queues on start', async () => {
        await client.initSchema()
        await client.queue.create('foo')
        await client.queue.create('faa', {
            max_attempts: 2,
            lock_duration: '1ms',
        })
        await client.start()

        expect(Object.keys(client.queues)).toEqual([ 'foo', 'faa' ])
        expect(client.queues.foo.max_attempts).toBe(5)
        expect(client.queues.foo.lock_duration).toBe('5m')
        expect(client.queues.faa.max_attempts).toBe(2)
        expect(client.queues.faa.lock_duration).toBe('1ms')
    })

    test('Fetchq should receive new settings as soon something change in the table', async () => {
        await client.initSchema()
        await client.queue.create('foo')
        await client.start()

        // @SQL: Simulate a change in the table that is not performed
        //       by the library itself.
        await client.query(`UPDATE "${client.schema}_catalog"."fq_queues" SET max_attempts = 1;`)
        await pause(10)

        expect(client.queues.foo.max_attempts).toBe(1)
    })
})