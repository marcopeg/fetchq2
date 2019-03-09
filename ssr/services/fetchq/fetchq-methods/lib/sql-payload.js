
import { FetchqNULL } from '../utils.null'
import { FetchqPayload } from '../utils.payload'

export const sqlPayload = (data = {}) => {
    if (
        data instanceof FetchqNULL
        || data instanceof FetchqPayload
    ) {
        return data.toString()
    }

    return `'${JSON.stringify(data).replace(/'/g, '\'\'\'\'')}'`
}
