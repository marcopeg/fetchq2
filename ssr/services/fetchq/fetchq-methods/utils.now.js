
export class FetchqNOW {
    toString () {
        return 'NOW()'
    }
}

export default ctx => () => new FetchqNOW()
