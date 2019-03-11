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
        //
        // Properties
        // =======================
        //

        // Sequelize compatible query runner.
        this.query = config.query || defaultQueryRunner

        // pg-pubsub instance
        this.pubsub = config.pubsub
        
        // keep an eye on a system that is ready
        this.isReady = false
        
        this.schema = config.schema || 'fetchq'

        // Will contain settings for each queue:
        // { foo: { maxAttempts: 5, ... }}
        this.queues = {}

        

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
}
