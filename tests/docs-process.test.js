import { getClient } from './get-client'

describe('Process Docs', () => {
    const qname = 'q1'
    let client
    
    beforeAll(async () => {
        client = await getClient()
    })

    beforeEach(async () => {
        await client.resetSchema()
        await client.initSchema()
        await client.queue.create(qname)
        await client.docs.insert(qname, [
            [ 't1', { id: 1 }, client.utils.now ],
            [ 't2', { id: 2 }, client.utils.plan('1s') ],
            [ 't3', { id: 2 }, client.utils.schedule('1s') ],
        ])
    })

    it('should be possible to pick documents in order', async () => {
        const docs = await client.docs.pick(qname, 3)
        const metrics = await client.metrics.get(qname)

        expect(docs.length).toBe(2)
        expect(docs[0].subject).toBe('t3')
        expect(docs[1].subject).toBe('t1')
        expect(docs[1]).toHaveProperty('next_iteration')
        expect(docs[1]).toHaveProperty('last_iteration', null)
        expect(docs[1].attempts).toBe(1)
        expect(docs[1].iterations).toBe(0)
        expect(docs[1].status).toBe(2)

        expect(metrics.pkd.value).toBe(2)
        expect(metrics.wip.value).toBe(2)
        expect(metrics.pnd.value).toBe(0)
        expect(metrics.pln.value).toBe(1)
    })

    it('should be possible to re-schedule a document', async () => {
        const docs = await client.docs.pick(qname)

        const res = await client.docs.schedule(qname, [{
            subject: docs[0].subject,
            payload: { ...docs[0].payload, completed: true },
            next_iteration: client.utils.plan('1s'),
        }])

        expect(res[0].subject).toBe(docs[0].subject)
        expect(res[0].payload).toHaveProperty('completed', true)
        expect(res[0]).toHaveProperty('next_iteration')
        expect(res[0].last_iteration).not.toBeNull()
        expect(res[0].iterations).toBe(1)
        expect(res[0].attempts).toBe(0)
        expect(res[0].status).toBe(0)
    })

})