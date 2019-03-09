import { INIT_SERVICE, START_SERVICE } from '@marcopeg/hooks'
import { query } from '../postgres'
import { SERVICE_NAME } from './hooks'
import { Fetchq } from './fetchq.class'

let client = null

export const register = ({ registerAction, createHook }) => {
    registerAction({
        hook: INIT_SERVICE,
        name: SERVICE_NAME,
        trace: __filename,
        handler: async () => {
            client = new Fetchq()
        },
    })

    registerAction({
        hook: START_SERVICE,
        name: SERVICE_NAME,
        trace: __filename,
        handler: async () => {
            client.setQueryFn(query)
        },
    })
}

export const getClient = () => {
    if (!client === null) {
        throw new Error('[Fetcqh] client not available yet')
    }
    return client
}
