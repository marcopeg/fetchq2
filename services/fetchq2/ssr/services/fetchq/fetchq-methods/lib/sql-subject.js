import { FetchqUUID } from '../utils/uuid'

export const sqlSubject = (value) => {
    if (typeof value === 'string') {
        return `'${value}'`
    }

    if (
        value instanceof FetchqUUID
    ) {
        return value.toString()
    }

    throw new Error(`[FetchQ] unrecognized value input for "subject"`)
}
