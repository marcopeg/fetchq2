
export class FetchqPlan {
    constructor (value) {
        this.value = value
    }

    toString () {
        return `NOW() + INTERVAL '${this.value}'`
    }
}

export default ctx => value => new FetchqPlan(value)
