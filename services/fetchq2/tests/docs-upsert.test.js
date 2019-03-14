import { getClient } from './get-client'

describe('Upsert Documents', () => {
    const qname = 'q1'
    let client
    
    beforeAll(async () => {
        client = await getClient()
        await client.start()
    })

    beforeEach(async () => {
        await client.resetSchema()
        await client.initSchema()
        await client.queue.create(qname)
    })

    it('should insert new documents with an upsert', async () => {
        const res = await client.docs.upsert(qname, [
            [ 't1', {}, client.utils.now ],
        ])

        expect(res).toHaveLength(1)
        expect(res[0].action).toEqual('created')
        expect(res[0].subject).toEqual('t1')
        expect(res[0].status).toEqual(1)  
    })

    it('should update an existing document during upsert', async () => {
        await client.docs.insert(qname, [
            [ 't1', {}, client.utils.now ],
        ])

        const res = await client.docs.upsert(qname, [
            [ 't1', {}, client.utils.plan('1y') ],
        ])

        expect(res).toHaveLength(1)
        expect(res[0].action).toEqual('updated')
        expect(res[0].subject).toEqual('t1')
        expect(res[0].status).toEqual(0)
    })

    it('should calculate metrics after upsert', async () => {
        await client.docs.insert(qname, [
            [ 't1', {}, client.utils.now ],
            [ 't2', {}, client.utils.plan('1m') ],
        ])

        await client.docs.upsert(qname, [
            [ 't1', {}, client.utils.plan('1m') ],
            [ 't2', {}, client.utils.now ],
            [ 't3', {}, client.utils.now ]
        ])

        const stats = await client.metrics.get(qname)
        expect(stats.cnt.value).toEqual(3)
        expect(stats.ent.value).toEqual(3)
        expect(stats.pnd.value).toEqual(2)
        expect(stats.pln.value).toEqual(1)
        expect(stats.upd.value).toEqual(2)
    })
})
