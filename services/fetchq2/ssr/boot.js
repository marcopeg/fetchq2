import * as config from '@marcopeg/utils/lib/config'
import {
    registerAction,
    createHookApp,
    logBoot,
    SETTINGS,
    FINISH,
} from '@marcopeg/hooks'

const services = [
    require('./services/env'),
    require('./services/logger'),
    require('./services/fetchq'),
]

const features = [
    require('./features/test0'),
    require('./features/test1'),
]

const getConfig = (key, def) => {
    const val = config.get(key, '---')
    return val === '---' ? def : val
}

registerAction({
    hook: SETTINGS,
    name: '♦ boot',
    handler: async ({ settings }) => {
        settings.feature = getConfig('RUN_FEATURE', 'test1')

        settings.test1 = {
            testSize: getConfig('TEST1_TEST_SIZE', 0),
            testType: getConfig('TEST1_TEST_TYPE', 'process'),
            processResetSchema: getConfig('TEST1_PROCESS_RESET_SCHEMA', false),
            processBulkInsert: getConfig('TEST1_PROCESS_BULK_INSERT', true),
            processUseMetrics: getConfig('TEST1_PROCESS_USE_METRICS', true),
            processSize: getConfig('TEST1_PROCESS_SIZE', 1000),
        }

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
