
export class FetchqLiteral {
    constructor (value) {
        this.value = value
    }

    toString () {
        return this.value
    }
}

export default ctx => value => new FetchqLiteral(value)
