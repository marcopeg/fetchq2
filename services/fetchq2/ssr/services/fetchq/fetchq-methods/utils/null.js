
export class FetchqNULL {
    toString () {
        return 'NULL'
    }
}

export default ctx => () => new FetchqNULL()
