import { getClient } from './get-client'

describe('Insert Documents', () => {
    const qname = 'q1'
    let client
    beforeAll(async () => {
        client = await getClient()
    })

    beforeEach(async () => {
        await client.resetSchema()
        await client.initSchema()
        await client.queue.create(qname)
    })

    it('should insert a document', async () => {
        const res = await client.docs.insert(qname, [
            [ 'foo', {}, client.utils.now ],
        ])

        expect(res).toHaveLength(1)
        expect(res[0].subject).toEqual('foo')
        expect(res[0].status).toEqual(1)
    })

    it('should skip existing documents during insert', async () => {
        // put a document
        await client.docs.insert(qname, [
            [ 'foo', {}, client.utils.now ],
        ])

        // put a duplicate and a new document
        const res = await client.docs.insert(qname, [
            [ 'foo', {}, client.utils.now ],
            [ 'faa', {}, client.utils.plan('1 m') ],
        ])

        expect(res).toHaveLength(1)
        expect(res[0].subject).toEqual('faa')
        expect(res[0].status).toEqual(0)
    })

    it('should calculate metrics after insert', async () => {
        await client.docs.insert(qname, [
            [ 't1', {}, client.utils.now ],
            [ 't2', {}, client.utils.plan('1 m') ],
            [ 't3', {}, client.utils.schedule('1 m') ],
        ])

        const stats = await client.metrics.get(qname)
        expect(stats.cnt.value).toEqual(3)
        expect(stats.ent.value).toEqual(3)
        expect(stats.pnd.value).toEqual(2)
        expect(stats.pln.value).toEqual(1)
    })
})
