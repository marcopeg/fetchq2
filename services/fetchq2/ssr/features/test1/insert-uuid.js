import { getClient } from 'services/fetchq'
import { logInfo } from 'services/logger'

const state = {
    options: {},
    poolMaxSize: 1000,
    poolSize: 0,
    batch: 100,
    nextInterval: 0,
    nextLoop: null,
    onComplete: null,
    iterations: 0,
    totalDuration: 0,
    avgDuration: 0,
    avgDurationStats: [],
    avgSpeed: 0,
    avgSpeedStats: [],
}

export const loop = async () => {
    const start = new Date()
    const client = getClient()

    const values = Array(state.batch).fill(0).map(_ => ([
        client.utils.uuid,
        client.utils.payload,
        Math.random() > 0.5 ? client.utils.plan('1y') : client.utils.now,
    ]))

    const res = await client.docs.insert('tasks', values, state.options)
    const duration = new Date() - start

    state.iterations += 1
    state.totalDuration += duration
    state.poolSize += res.length
    state.avgDuration = Math.round(state.totalDuration / state.iterations)
    state.avgSpeed = Math.floor(state.poolSize * 1000 / state.totalDuration)
    // logInfo(`[test1] ${duration}ms - Insert UUID ${res.length} of ${state.batch} unique documents - ${state.poolSize}`)

    if (state.poolSize < state.poolMaxSize) {
        state.nextLoop = setTimeout(loop, state.nextInterval)
    } else {
        logInfo(`[test1] Ingest UUID - DONE in ${state.totalDuration}ms, average speed: ${state.avgSpeed} docs/s`)
        if (state.onComplete) state.onComplete()
    }
}

export const start = (pool = 1000, batch = 100, options = {}) => {
    state.poolMaxSize = pool
    state.batch = batch
    state.options = options

    // reset local state
    clearInterval(state.nextLoop)
    state.poolSize = 0
    state.iterations = 0
    state.totalDuration = 0
    state.avgDuration = 0
    state.avgDurationStats = []
    state.avgSpeed = 0
    state.avgSpeedStats = []

    loop()
    return new Promise((resolve) => {
        const log = setInterval(() => {
            const progress = Math.round(state.poolSize / state.poolMaxSize * 100)
            state.avgDurationStats.push(state.avgDuration)
            state.avgSpeedStats.push(state.avgSpeed)
            logInfo(`[test1] Ingest UUID - ${progress}% in ${state.totalDuration}ms, average speed: ${state.avgSpeed} docs/s`)
        }, 1000)

        state.onComplete = () => {
            clearInterval(log)
            resolve(state)
        }
    })
}

export const stop = () => {
    clearTimeout(state.nextLoop)
}

export const getState = () => ({
    options: state.options,
    poolMaxSize: state.poolMaxSize,
    poolSize: state.poolSize,
    batch: state.batch,
    iterations: state.iterations,
    totalDuration: state.totalDuration,
    avgDuration: state.avgDuration,
    avgDurationStats: state.avgDurationStats,
    avgSpeed: state.avgSpeed,
    avgSpeedStats: state.avgSpeedStats,
})
