import { getClient } from './get-client'

describe('metrics', () => {
    const qname = 'q1'
    let client
    
    beforeAll(async () => {
        client = await getClient()
        await client.start()
    })

    beforeEach(async () => {
        await client.resetSchema()
        await client.start()
        await client.queue.create(qname)
    })
    test('It should compute real metrics', async () => {
        await client.docs.upsert(qname, [
            [ 'task1', client.utils.json, client.utils.now ],
            [ client.utils.uuid, client.utils.json, client.utils.plan('1s') ],
            [ client.utils.uuid, client.utils.json, client.utils.schedule('1s') ],
        ])
        await client.docs.pick(qname, 1, { 'lock': '1ms' })

        // @TODO: generate completed and killed

        const stats = await client.metrics.get(qname)
        const statsRealTime = await client.metrics.compute(qname)

        expect(stats.cnt.value).toBe(statsRealTime.cnt.value)
        expect(stats.pnd.value).toBe(statsRealTime.pnd.value)
        expect(stats.pln.value).toBe(statsRealTime.pln.value)
        expect(stats.wip.value).toBe(statsRealTime.wip.value)
    })
})
