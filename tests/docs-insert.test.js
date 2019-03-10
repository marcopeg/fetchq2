import { getClient } from './get-client'

describe('Insert Documents', () => {
    let client
    beforeAll(async () => {
        client = await getClient()
    })

    beforeEach(async () => {
        await client.resetSchema()
        await client.initSchema()
        await client.queue.create('q1')
    })

    it('should work', async () => {
        await client.docs.insert('q1', [
            [ 'foo', {}, client.utils.now ],
        ])
    })
})
