
export class FetchqUUID {
    toString () {
        return 'uuid_generate_v4()::text'
    }
}

export default ctx => () => new FetchqUUID()
