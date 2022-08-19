import pg from "pg";
import _ from "lodash";

export interface IDBConfig {
    database?: string;
    user?: string;
    password?: string;
    host?: string;
    port?: number;
    unfreeze?: boolean;
}

const defaultConfig: IDBConfig = {
    database: undefined,
    user: undefined,
    password: undefined,
    host: "localhost",
    port: 5432,
    unfreeze: false
};

export async function getDbClient(dbConfig: IDBConfig | pg.Pool) {
    if ( "query" in dbConfig ) {
        return dbConfig;
    }

    const config = parseDbConfig(dbConfig);
    const DAY = 24 * 60 * 60 * 1000;

    const pool = new pg.Pool({
        ...config,
        idleTimeoutMillis: DAY,
        connectionTimeoutMillis: DAY
    });
    return pool;
}

function parseDbConfig(inputConfig: IDBConfig) {
    const config = _.clone(defaultConfig);

    if ( "database" in inputConfig ) {
        config.database = inputConfig.database;
    }
    if ( "user" in inputConfig ) {
        config.user = inputConfig.user;
    }
    if ( "password" in inputConfig ) {
        config.password = inputConfig.password;
    }
    if ( "host" in inputConfig ) {
        config.host = inputConfig.host;
    }
    if ( "port" in inputConfig ) {
        config.port = inputConfig.port;
    }

    return config;
}
