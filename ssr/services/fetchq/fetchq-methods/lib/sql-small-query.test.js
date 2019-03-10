import { sqlSmallQuery } from './sql-small-query'

describe('sqlSmallQuery', () => {
    const ctx = {
        schema: 'public',
    }

    it('should optimize a query', () => {
        const query = `
            SELECT *
            -- this is a comment
            FROM :schemaName.tableName
        `

        const [ _query ] = sqlSmallQuery(ctx, query)
        expect(_query).toBe('SELECT * FROM public.tableName')
    })

    it('should left things almost untouched in debug', () => {
        const query = `
            SELECT *
            -- this is a comment
            FROM :schemaName.tableName
        `

        const [ _query ] = sqlSmallQuery(ctx, query, true)
        expect(_query).toBe(`
            SELECT *
            -- this is a comment
            FROM public.tableName
        `)
    })

    it('should work on multiple queries', () => {
        const q1 = `
            SELECT *
            -- this is a comment
            FROM :schemaName.tableName
        `

        const q2 = `
            SELECT *
            -- this is a comment
            FROM :schemaName.tableName
        `

        const [ _q1, _q2 ] = sqlSmallQuery(ctx, q1, q2)
        expect(q1).toEqual(q2)
    })
})
