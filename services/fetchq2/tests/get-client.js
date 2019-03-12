import { Fetchq } from '../ssr/services/fetchq/fetchq.class'
import { logVerbose } from '@marcopeg/utils/lib/logger'
import pause from '@marcopeg/utils/lib/pause'

let client

export const getClient = async () => {
    // Return memoized client
    if (client) {
        return client
    }

    // Init Fetchq Client
    client = new Fetchq({
        schema: 'fetchq_jest',
        server: {
            host: process.env.PG_HOST,
            port: process.env.PG_PORT,
            database: process.env.PG_DATABASE,
            username: process.env.PG_USERNAME,
            password: process.env.PG_PASSWORD,
            maxAttempts: Number(process.env.PG_MAX_CONN_ATTEMPTS),
            attemptDelay: Number(process.env.PG_CONN_ATTEMPTS_DELAY),
            logging: logVerbose,
        }
    })

    await client.connect()

    // Fix the log that pg-pubsub triggers on reconnect
    client.subscribe('test', () => {})
    await pause(100)

    return client
}
