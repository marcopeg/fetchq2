
export class FetchqPayload {
    toString () {
        return `'{}'`
    }
}

export default ctx => () => new FetchqPayload()
