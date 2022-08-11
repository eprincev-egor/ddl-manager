import pg from "pg";

export async function createCallsTable(db: pg.Pool) {
    
    console.log("creating temp table");
    await db.query(`
        drop table if exists system_calls;
        
        CREATE TABLE system_calls
        (
            id serial primary key,
            tid integer,
            func_name text,
            call_time numeric,
            end_time numeric,
            end_id integer unique
        );
    `);
}

export async function downloadLogs(db: pg.Pool) {

    console.log("download logs");
    let logsResult;
    try {
        logsResult = await db.query(`
            select *
            from system_calls
            order by id
        `);
    } catch(err) {
        console.error(err);
        throw new Error("cannot download logs");
    }

    return logsResult.rows;
}

export async function clearCallsLogs(db: pg.Pool) {
    
    console.log("clear calls from system_calls");
    await db.query(`
        delete from system_calls;
    `);
}
