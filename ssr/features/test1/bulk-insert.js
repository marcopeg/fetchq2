import prettyMs from 'pretty-ms'
import { logInfo } from 'services/logger'
import { getClient } from 'services/fetchq'

import * as insertUUID from './insert-uuid'
import * as insertCollide from './insert-collide'
import * as upsertUUID from './upsert-uuid'
import * as upsertCollide from './upsert-collide'

export const bulkInsert = async (config = {}) => {
    // Default settings
    const poolSize = config.poolSize || 10000
    const batchSize = config.batchSize || 1000
    const useMetrics = config.useMetrics === undefined ? true : config.useMetrics
    const resetSchema = config.resetSchema === undefined ? true : config.resetSchema
    const useIndex = config.dropIndex === undefined ? true : config.dropIndex

    // Report schema
    const report = {
        poolSize,
        batchSize,
        useMetrics,
        resetSchema,
        useIndex,
        error: null,
        timers: {
            startTest: null,
            endTest: null,
            startInsert: null,
            endInsert: null,
        },
        durations: {
            test: 0,
            insert: 0,
        },
        results: {
            insertUUID: null,
            insertCollide: null,
            upsertUUID: null,
            upsertCollide: null,
        },
    }

    try {
        const client = getClient()
        let stats

        // Prepare the schema
        report.timers.startTest = new Date()
        if (resetSchema) await client.resetSchema()
        await client.initSchema()
        await client.queue.create('tasks')

        // Handle indexes
        if (useIndex) {
            await client.queue.index('tasks')
        } else {
            await client.queue.dropIndex('tasks')
        }

        // Run bulk insert
        report.timers.startInsert = new Date()
        await Promise.all([
            insertUUID.start(poolSize, batchSize, { metrics: useMetrics }),
            insertCollide.start(poolSize, batchSize, { metrics: useMetrics }),
            upsertUUID.start(poolSize, batchSize, { metrics: useMetrics }),
            upsertCollide.start(poolSize, batchSize, { metrics: useMetrics }),
        ])
        report.timers.endInsert = new Date()

        // Get the end statuses
        report.results.insertUUID = insertUUID.getState()
        report.results.insertCollide = insertCollide.getState()
        report.results.upsertUUID = upsertUUID.getState()
        report.results.upsertCollide = upsertCollide.getState()
        

        report.timers.endTest = new Date()

        report.durations.test = report.timers.endTest - report.timers.startTest
        report.durations.insert = report.timers.endInsert - report.timers.startInsert

    } catch (err) {
        report.timers.endTest = new Date()
        report.error = err
    }

    return report
}


export const avgSpeed = data => {
    const tot = ([
        data.results.insertUUID.avgSpeed,
        data.results.insertCollide.avgSpeed,
        data.results.upsertUUID.avgSpeed,
        data.results.upsertCollide.avgSpeed,
    ]).reduce((acc, curr) => (acc + curr), 0)

    return Math.round(tot / 4)
}

export const avgSpeedInsert = data => {
    const tot = ([
        data.results.insertUUID.avgSpeed,
        data.results.insertCollide.avgSpeed,
    ]).reduce((acc, curr) => (acc + curr), 0)

    return Math.round(tot / 2)
}

export const avgSpeedUpsert = data => {
    const tot = ([
        data.results.upsertUUID.avgSpeed,
        data.results.upsertCollide.avgSpeed,
    ]).reduce((acc, curr) => (acc + curr), 0)

    return Math.round(tot / 2)
}

export const logReport = (name, data) => {
    logInfo('')
    logInfo('')
    logInfo(`--- BULK INSERT [${name.toUpperCase()}] ---`)
    logInfo('## Settings:')
    logInfo(`pool size:     ${data.poolSize}`)
    logInfo(`batch size:    ${data.batchSize}`)
    logInfo(`use metrics:   ${data.useMetrics}`)
    logInfo(`reset schema:  ${data.resetSchema}`)
    logInfo('')
    logInfo('## Results:')
    logInfo(`error:         ${data.error ? data.error.message : 'no errors'}`)
    logInfo(`total time:    ${prettyMs(data.durations.test)}`)
    logInfo(`insert time:   ${prettyMs(data.durations.insert)}`)
    logInfo(`avg. speed:    ${avgSpeed(data)} docs/s`)
    logInfo('')
    logInfo('## Insert:')
    logInfo(`avg. speed:   ${avgSpeedInsert(data)} docs/s`)
    logInfo('')
    logInfo('## Upsert:')
    logInfo(`avg. speed:   ${avgSpeedUpsert(data)} docs/s`)
    logInfo('')
    logInfo('')
}