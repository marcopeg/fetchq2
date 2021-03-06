import { INIT_SERVICE, START_SERVICE } from '@marcopeg/hooks'
import { SERVICE_NAME } from './hooks'
import { Fetchq } from './fetchq.class'

let client = null

export const init = (options) => {
    client = new Fetchq(options.fetchq)
}

export const start = async () => {
    await client.connect()
    await client.start()
}

export const register = ({ registerAction }) => {
    registerAction({
        hook: INIT_SERVICE,
        name: SERVICE_NAME,
        trace: __filename,
        handler: init,
    })

    registerAction({
        hook: START_SERVICE,
        name: SERVICE_NAME,
        trace: __filename,
        handler: start,
    })
}

export const getClient = () => {
    if (!client === null) {
        throw new Error('[Fetcqh] client not available yet')
    }
    return client
}
