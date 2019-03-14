import { getClient } from './get-client'

describe('Concurrency test', () => {
    const qname = 'q1'
    let client

    const generateUniqueDocs = amount => Array(amount).fill(0).map(_ => ([
        client.utils.uuid,
        client.utils.payload,
        Math.random() > 0.5 ? client.utils.plan('1y') : client.utils.now,
    ]))
    
    const generateDocs = (amount, variance) => Array(amount).fill(0).map(_ => ([
        `task-${Math.floor(Math.random() * Math.floor(variance))}`,
        client.utils.payload,
        Math.random() > 0.5 ? client.utils.plan('1y') : client.utils.now,
    ]))

    const verifyRealTimeStats = async () => {
        const stats = await client.metrics.get(qname)
        const stats_rt = await client.metrics.compute(qname)
        expect(stats.cnt.value).toBe(stats_rt.cnt.value)
        expect(stats.pnd.value).toBe(stats_rt.pnd.value)
        expect(stats.pln.value).toBe(stats_rt.pln.value)
        expect(stats.wip.value).toBe(stats_rt.wip.value)
        expect(stats.kll.value).toBe(stats_rt.kll.value)
        expect(stats.cpl.value).toBe(stats_rt.cpl.value)
    }

    beforeAll(async () => {
        client = await getClient()
        await client.start()
    })

    beforeEach(async () => {
        await client.resetSchema()
        await client.initSchema()
        await client.queue.create(qname)
    })

    test('serie - insert unique documents', async () => {
        await client.docs.insert(qname, generateUniqueDocs(1000))
        await client.docs.insert(qname, generateUniqueDocs(1000))

        const stats = await client.metrics.get(qname)
        expect(stats.cnt.value).toEqual(2000)
        expect(stats.ent.value).toEqual(2000)

        await verifyRealTimeStats()
    })
    
    test('parallel - insert unique documents', async () => {
        await Promise.all([
            client.docs.insert(qname, generateUniqueDocs(1000)),
            client.docs.insert(qname, generateUniqueDocs(1000)),
        ])

        const stats = await client.metrics.get(qname)
        expect(stats.cnt.value).toEqual(2000)
        expect(stats.ent.value).toEqual(2000)

        await verifyRealTimeStats()
    })

    test('serie - insert colliding documents', async () => {
        await client.docs.insert(qname, generateDocs(1000, 10000))
        await client.docs.insert(qname, generateDocs(1000, 10000))
        await verifyRealTimeStats()
    })
    
    test('parallel - insert colliding documents', async () => {
        await Promise.all([
            client.docs.insert(qname, generateDocs(10000, 100)),
            client.docs.insert(qname, generateDocs(10000, 100)),
            client.docs.insert(qname, generateDocs(10000, 100)),
            client.docs.insert(qname, generateDocs(10000, 100)),
        ])

        await verifyRealTimeStats()
    })
})