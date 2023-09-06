import pg from "pg";

interface IConfig {
    database: string | false;
    user: string | false;
    password: string | false;
    host: string;
    port: number;
}

export async function getDBClient(dbConfig: IConfig = {
    database: false,
    user: false,
    password: false,
    host: "localhost",
    port: 5432
}) {
    if ( !dbConfig.database || !dbConfig.user ) {
        let dbConfigFromFile;
        try {
            dbConfigFromFile = require("../../ddl-manager-config");
        } catch(err) {
            throw new Error("Please make file ddl-manager-config.js in project. Modules must return object, with properties: database, user, password, host, port");
        }
    
        if ( dbConfigFromFile.database ) {
            dbConfig.database = dbConfigFromFile.database;
        }
        if ( dbConfigFromFile.user ) {
            dbConfig.user = dbConfigFromFile.user;
        }
        if ( dbConfigFromFile.password ) {
            dbConfig.password = dbConfigFromFile.password;
        }
        if ( dbConfigFromFile.host ) {
            dbConfig.host = dbConfigFromFile.host;
        }
        if ( dbConfigFromFile.port ) {
            dbConfig.port = dbConfigFromFile.port;
        }
    
    }
    
    if ( !dbConfig.user && !dbConfig.password && !dbConfig.database ) {
        throw new Error("Connect options does not exists!");
    }

    const pool = new pg.Pool({
        ...(dbConfig as any),
        min: 16,
        max: 100
    });
    try {
        await pool.query("select 1");
    } catch(err: any) {
        throw new Error(
            "Failed db connection: " + 
            JSON.stringify(dbConfig, null, 4) + 
            "\n" +
            "\n" +
            err.message
        );
    }

    // redefine errors callstack
    const query = pool.query as any;
    pool.query = async function(...args: any[]) {
        const stack = new Error().stack;
        try {
            return await query.call(pool, ...args);
        } catch(error: any) {
            error.stack = (error.where || "") + stack;
            throw error;
        }
    } as any;

    return pool;
}

