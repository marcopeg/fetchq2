import { getClient } from './get-client'

describe('Create Queue', () => {
    let client
    beforeAll(async () => {
        client = await getClient()
        await client.start()
    })

    beforeEach(async () => {
        await client.resetSchema()
        await client.initSchema()
    })

    it('should work', async () => {
        await client.queue.create('q1')
    })
})
