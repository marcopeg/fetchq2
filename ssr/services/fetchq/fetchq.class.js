import resetSchema from './fetchq-methods/reset-schema'
import initSchema from './fetchq-methods/init-schema'

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

export class Fetchq {
    constructor (config = {}) {
        this.setSchema(config.schema)
        this.query = () => {
            throw new Error('[Fetcqh] is missing a query method')
        }

        this.initSchema = initSchema(this)
        this.resetSchema = resetSchema(this)

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

    setSchema (schemaName = 'public') {
        this.schema = schemaName
    }

    setQueryFn (query) {
        this.query = query
    }
}
