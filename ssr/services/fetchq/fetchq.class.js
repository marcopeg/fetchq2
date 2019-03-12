import { Pool, Client } from 'pg'
import PgPubsub from 'pg-pubsub'
import { logError, logDebug } from '@marcopeg/utils/lib/logger'
import pause from '@marcopeg/utils/lib/pause'

import resetSchema from './fetchq-methods/lifecycle/reset-schema'
import initSchema from './fetchq-methods/lifecycle/init-schema'
import dropSchema from './fetchq-methods/lifecycle/drop-schema'
import start from './fetchq-methods/lifecycle/start'

import createQueue from './fetchq-methods/queue/create'
import indexQueue from './fetchq-methods/queue/index-create'
import dropQueueIndex from './fetchq-methods/queue/index-drop'

import mntOrphans from './fetchq-methods/queue/mnt-orphans'

import insertDocuments from './fetchq-methods/documents/insert'
import upsertDocuments from './fetchq-methods/documents/upsert'
import pickDocument from './fetchq-methods/documents/pick'
import scheduleDocument from './fetchq-methods/documents/schedule'
import completeDocument from './fetchq-methods/documents/complete'
import killDocument from './fetchq-methods/documents/kill'

import getMetrics from './fetchq-methods/metrics/get'
import computeMetrics from './fetchq-methods/metrics/compute'

import utilsPlan from './fetchq-methods/utils/plan'
import utilsSchedule from './fetchq-methods/utils/schedule'
import utilsLiteral from './fetchq-methods/utils/literal'
import utilsNow from './fetchq-methods/utils/now'
import utilsUuid from './fetchq-methods/utils/uuid'
import utilsNull from './fetchq-methods/utils/null'
import utilsPayload from './fetchq-methods/utils/payload'

const defaultQueryRunner = () => {
    throw new Error('[Fetcqh] is missing a query method')
}

export class Fetchq {
    constructor (config = {}) {
        // @TODO: config validation with json-schema or similar?

        //
        // Properties
        // =======================
        //

        // Stuff from the settings
        this.schema = config.schema || 'fetchq'
        
        // keep an eye on a system that is ready
        this.isReady = false
        
        // Will contain settings for each queue:
        // { foo: { maxAttempts: 5, ... }}
        this.queues = {}

        // Handle internal references to the connection pool
        this.conn = {
            settings: config.server || {},
            pool: null, // postgres connection
            pubsub: null, // pg-pubsub connection
        }
        

        //
        // Methods
        // =======================
        // 

        this.initSchema = initSchema(this)
        this.dropSchema = dropSchema(this)
        this.resetSchema = resetSchema(this)
        this.start = start(this)

        this.queue = {
            create: createQueue(this),
            index: indexQueue(this),
            dropIndex: dropQueueIndex(this),
            mnt: {
                orphans: mntOrphans(this),
            }
        }

        this.docs = {
            insert: insertDocuments(this),
            upsert: upsertDocuments(this),
            pick: pickDocument(this),
            schedule: scheduleDocument(this),
            complete: completeDocument(this),
            kill: killDocument(this),
        }

        this.metrics = {
            get: getMetrics(this),
            compute: computeMetrics(this),
        }

        this.utils = {
            now: utilsNow(this)(),
            uuid: utilsUuid(this)(),
            null: utilsNull(this)(),
            payload: utilsPayload(this)(),
            plan: utilsPlan(this),
            schedule: utilsSchedule(this),
            literal: utilsLiteral(this),
        }
    }

    // Connects both "pg" and "pg-pubsub"
    async connect () {
        const { username, password, host, port, database } = this.conn.settings
        const { maxAttempts, attemptDelay } = this.conn.settings

        this.conn.string = [
            'postgresql://',
            username,
            password ? `:${password}` : '',
            '@',
            host,
            port ? `:${port}` : '',
            database ? `/${database}` : '',
        ].join('')

        this.conn.pool = new Pool({
            connectionString: this.conn.string,
        })

        let attempts = 0
        let lastError = ''
        let connected = false
        do {
            try {
                logDebug(`[fetchq] pg connection attempt ${attempts + 1}/${maxAttempts}"`)
                await this.conn.pool.query('SELECT 1 = 1')
                connected = true
            } catch (err) {
                attempts += 1
                lastError = err
                logDebug(`[fetchq] failed connection attempt: ${err.message}`)
                await pause(attemptDelay)
            }
        } while (connected === false && attempts < maxAttempts)

        if (!connected) {
            const safeConnString = this.conn.string.replace(`:${password}@`, ':xxx@')
            const errMsg = `[fetchq] failed db connection: "${safeConnString}" - ${lastError.message}`
            const err = new Error(errMsg)
            err.connectionString = this.conn.string
            err.original = lastError
            logError(errMsg)
            logDebug(err)
            throw new Error(err)
        }
        
        this.conn.pubsub = new PgPubsub(this.conn.string)
    }

    query (q, p) {
        return this.conn.pool.query(q, p)
    }

    subscribe (evtName, handler) {
        return this.conn.pubsub.addChannel(evtName, handler)
    }
}
