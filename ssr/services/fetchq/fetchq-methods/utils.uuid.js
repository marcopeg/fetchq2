
export class FetchqUUID {
    toString () {
        return 'uuid_generate_v4()'
    }
}

export default ctx => () => new FetchqUUID()
