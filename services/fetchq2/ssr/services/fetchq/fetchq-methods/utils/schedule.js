
export class FetchqSchedule {
    constructor (value) {
        this.value = value
    }

    toString () {
        return `NOW() - INTERVAL '${this.value}'`
    }
}

export default ctx => value => new FetchqSchedule(value)
