import { logError } from 'services/logger'

export const test = (msg, fn) => {
    try {
        fn()
    } catch (err) {
        logError(`[TEST FAILED] ${msg}`)
        console.log(err.message)
        throw new Error('failed tests')
    }
}
