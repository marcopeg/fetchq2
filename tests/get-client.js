import * as pg from '../ssr/services/postgres'
import * as fetchq from '../ssr/services/fetchq'

let client

export const getClient = async () => {
    // Return memoized client
    if (client) {
        return client
    }

    // Init Postgres Connection
    const options = {
        connectionName: 'default',
        host: process.env.PG_HOST,
        port: process.env.PG_PORT,
        database: process.env.PG_DATABASE,
        username: process.env.PG_USERNAME,
        password: process.env.PG_PASSWORD,
        maxAttempts: Number(process.env.PG_MAX_CONN_ATTEMPTS),
        attemptDelay: Number(process.env.PG_CONN_ATTEMPTS_DELAY),
        logging: () => {},
        models: [],
    }
    await pg.init(options)
    await pg.start(options)

    // Init Fetchq Client
    await fetchq.init({
        schema: 'fetchq_jest',
    })
    await fetchq.start()

    // Memoize the client
    client = fetchq.getClient()
    return client
}
