import pause from '@marcopeg/utils/lib/pause'
import { getClient } from './get-client'

describe('Queue Maintenance', () => {
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
        await client.docs.insert(qname, [
            [ 't1', { id: 1 }, client.utils.now ],
            [ 't2', { id: 2 }, client.utils.plan('1s') ],
            [ 't3', { id: 2 }, client.utils.schedule('1s') ],
        ])
    })

    it('should reschedule an orphan', async () => {
        await client.docs.pick(qname, 1, { lock: '1ms' })
        await pause(2)
        await client.queue.mnt.orphans(qname)

        const metrics = await client.metrics.get(qname)
        expect(metrics.pkd.value).toBe(1)
        expect(metrics.pnd.value).toBe(2)
    })
})