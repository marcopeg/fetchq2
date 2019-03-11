import path from 'path'
import fs from 'fs-extra'
import glob from 'glob'
import uuid from 'uuid/v1'
import * as config from '@marcopeg/utils/lib/config'
import { logInfo, logVerbose, logDebug } from 'services/logger'
import {
    createHook,
    registerAction,
    createHookApp,
    logBoot,
    SETTINGS,
    FINISH,
} from '@marcopeg/hooks'

const services = [
    require('./services/env'),
    require('./services/logger'),
    // require('./services/postgres'),
    require('./services/fetchq'),
]

const features = [
    // require('./features/test0'),
    // require('./features/test1'),
]

registerAction({
    hook: SETTINGS,
    name: '♦ boot',
    handler: async ({ settings }) => {
        settings.fetchq = {
            schema: 'fetchq_app',
            server: {
                host: config.get('PG_HOST'),
                port: config.get('PG_PORT'),
                database: config.get('PG_DATABASE'),
                username: config.get('PG_USERNAME'),
                password: config.get('PG_PASSWORD'),
                maxAttempts: Number(config.get('PG_MAX_CONN_ATTEMPTS')),
                attemptDelay: Number(config.get('PG_CONN_ATTEMPTS_DELAY')),
                // logging: logVerbose,
            }
        }

        settings.postgres = [{
            connectionName: 'default',
            host: config.get('PG_HOST'),
            port: config.get('PG_PORT'),
            database: config.get('PG_DATABASE'),
            username: config.get('PG_USERNAME'),
            password: config.get('PG_PASSWORD'),
            maxAttempts: Number(config.get('PG_MAX_CONN_ATTEMPTS')),
            attemptDelay: Number(config.get('PG_CONN_ATTEMPTS_DELAY')),
            // logging: logVerbose,
            models: [],
        }]
    },
})

registerAction({
    hook: FINISH,
    name: '♦ boot',
    handler: () => logBoot(),
})

export default createHookApp({
    settings: { cwd: process.cwd() },
    services,
    features,
})
