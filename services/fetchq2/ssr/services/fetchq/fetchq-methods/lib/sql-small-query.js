/**
 * Taks an SQL query, compress it and fills in the :schemaName
 * from the Fetch context.
 * 
 * Run it with a "true" as last value to skip compression
 */

export const sqlSmallQuery = (ctx, ...queries) => {
    // debug mode
    if (queries[queries.length - 1] === true) {
        queries.pop()
        return queries.map(query => (
            query.replace(/:schemaName/g, ctx.schema)
        ))    
    }

    // production mode
    return queries.map(query => (
        query
            .split('\n')
            .map(line => line.trim())
            .filter(line => line.substr(0, 2) !== '--')
            .join(' ')
            .replace(/:schemaName/g, ctx.schema)
            .replace(/\ \ \ \ /g, '')
            .replace(/\n/g, ' ')
            .trim()
    ))
}