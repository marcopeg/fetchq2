
import { sqlSubject } from '../lib/sql-subject'
import { sqlPayload } from '../lib/sql-payload'
import { sqlNextIteration } from '../lib/sql-next-iteration'
import { sqlSmallQuery } from '../lib/sql-small-query'

const q = `
WITH
all_docs (subject, payload, next_iteration, status) AS (
	SELECT
		origin.*, 
		CASE WHEN origin.next_iteration <= NOW() THEN 1
             ELSE 0
        END AS status 
	FROM ( VALUES :values ) AS origin (subject, payload, next_iteration)
),
inserted_docs AS (
    INSERT INTO ":schemaName_data".":queueName__docs" (subject, payload, next_iteration, status)
    SELECT subject, payload::jsonb, next_iteration, status
    FROM all_docs
    ON CONFLICT (subject) DO NOTHING
    RETURNING *
),
skipped_docs AS (
    SELECT
        _source.*, 
        _values.payload AS payload_new,
        _values.next_iteration AS next_iteration_new,
        _values.status AS status_new,
        _source.status AS status_old
    FROM ":schemaName_data".":queueName__docs" as _source
    JOIN all_docs AS _values ON _source.subject = _values.subject
    WHERE _source.subject NOT IN (SELECT subject FROM inserted_docs)
    FOR UPDATE SKIP LOCKED
),
updated_docs AS (
    UPDATE ":schemaName_data".":queueName__docs" AS docs
    SET
        payload = _value.payload_new::jsonb,
        next_iteration = _value.next_iteration_new,
        status = _value.status_new
    FROM (
        SELECT * FROM skipped_docs
    ) AS _value
    WHERE docs.subject = _value.subject
    RETURNING 
        _value.subject,
        docs.payload,
        docs.status,
        _value.attempts,
        _value.iterations,
        docs.next_iteration,
        _value.last_iteration,
        _value.status AS status_old,
        docs.status AS status_new
),
results AS (
    SELECT
        'updated' AS action,
        t1.subject,
        t1.payload,
        t1.status,
        t1.attempts,
        t1.iterations,
        t1.next_iteration,
        t1.last_iteration
    FROM updated_docs AS t1
    UNION ALL
    SELECT
        'created' AS action,
        t2.subject,
        t2.payload,
        t2.status,
        t2.attempts,
        t2.iterations,
        t2.next_iteration,
        t2.last_iteration
    FROM inserted_docs AS t2
)
:queryStats
SELECT * FROM results
`

const qStats = `
,
count_inserted AS (
    SELECT COUNT(subject) AS amount FROM inserted_docs
),
increment_cnt AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT MAX(amount) FROM count_inserted),
    last_update = NOW()
    WHERE metric = 'cnt'
),
increment_ent AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT MAX(amount) FROM count_inserted),
    last_update = NOW()
    WHERE metric = 'ent'
),
increment_upd AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (SELECT COUNT(subject) FROM updated_docs),
    last_update = NOW()
    WHERE metric = 'upd'
),
increment_pln AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (
        (
            SELECT COUNT(subject) FROM inserted_docs
            WHERE status = 0
        )
        +
        (
            SELECT COUNT(subject) FROM updated_docs
            WHERE status_old = 1
            AND status_new = 0
        )
        -
        (
            SELECT COUNT(subject) FROM updated_docs
            WHERE status_old = 0
            AND status_new = 1
        )
    ),
    last_update = NOW()
    WHERE metric = 'pln'
),
increment_pnd AS (
    UPDATE ":schemaName_data".":queueName__metrics"
    SET amount = amount + (
        (
            SELECT COUNT(subject) FROM inserted_docs
            WHERE status  = 1
        )
        +
        (
            SELECT COUNT(subject) FROM updated_docs
            WHERE status_old = 0
            AND status_new  = 1
        )
        -
        (
            SELECT COUNT(subject) FROM updated_docs
            WHERE status_old  = 1
            AND status_new = 0
        )
    ),
    last_update = NOW()
    WHERE metric = 'pnd'
)`

const doc2str = (acc, doc) => {
    const subject = sqlSubject(doc[0])
    const payload =  sqlPayload(doc[1])
    const nextIteration = sqlNextIteration(doc[2])
    return acc + `,(${subject},${payload},${nextIteration})`
}

export default ctx => {
    const [ _q, _qStats ] = sqlSmallQuery(ctx, q, qStats)

    return async (queueName, docs, options = {}) => {
        try {
            const values = docs.reduce(doc2str, '').substr(1)
            const query = _q
                .replace(/:queueName/g, queueName)
                .replace(/:values/g, values)
                .replace(/:queryStats/g, options.metrics === false ? '' : (
                    _qStats.replace(/:queueName/g, queueName)
                ))
            
            const res = await ctx.query(query)

            return res.rows
        } catch (err) {
            const error = new Error(`[Fetchq] failed to insert documents: ${queueName} - ${err.message}`)
            error.original = err
            throw error
        }
    }    
}