import moment from 'moment'
import { FetchqPlan } from '../utils/plan'
import { FetchqSchedule } from '../utils/schedule'
import { FetchqLiteral } from '../utils/literal'
import { FetchqNOW } from '../utils/now'

export const sqlNextIteration = (data) => {
    if (typeof data === 'string') {
        return `'${data}'`
    }

    if (data instanceof Date) {
        return `'${moment(data).format('YYYY-MM-DD HH:mm:ss Z')}'`
    }

    if (
        data instanceof FetchqPlan
        || data instanceof FetchqSchedule
        || data instanceof FetchqLiteral
        || data instanceof FetchqNOW
    ) {
        return data.toString()
    }

    throw new Error(`[FetchQ] unrecognized data input for "next_iteration"`)
}
