import resetSchema from './fetchq-methods/reset-schema'
import initSchema from './fetchq-methods/init-schema'
import createQueue from './fetchq-methods/create-queue'
import insertDocuments from './fetchq-methods/insert-documents'
import upsertDocuments from './fetchq-methods/upsert-documents'
import utilsPlan from './fetchq-methods/utils.plan'
import utilsLiteral from './fetchq-methods/utils.literal'
import utilsNow from './fetchq-methods/utils.now'
import utilsUuid from './fetchq-methods/utils.uuid'
import utilsNull from './fetchq-methods/utils.null'
import utilsPayload from './fetchq-methods/utils.payload'

export class Fetchq {
    constructor () {
        this.schema = 'fetchq'
        this.query = () => {
            throw new Error('[Fetcqh] is missing a query method')
        }

        this.initSchema = initSchema(this)
        this.resetSchema = resetSchema(this)
        this.createQueue = createQueue(this)
        this.insert = insertDocuments(this)
        this.upsert = upsertDocuments(this)

        this.utils = {
            now: utilsNow(this)(),
            uuid: utilsUuid(this)(),
            null: utilsNull(this)(),
            payload: utilsPayload(this)(),
            plan: utilsPlan(this),
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
