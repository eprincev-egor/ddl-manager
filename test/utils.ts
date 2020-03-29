import { DBDriver } from "../lib/db/DBDriver";

export async function sleep(ms: number) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

export function readDatabaseOptions(): DBDriver["options"] {
    let dbConfig = {
        host: "localhost",
        port: 5432,
        database: null,
        user: null,
        password: null,
    };
    
    let databaseJSON;
    try {
        databaseJSON = require("../database.json");
    } catch(err) {
        throw new Error("Please make file database.json in root.");
    }

    const isValidDatabaseJSON = (
        databaseJSON &&
        databaseJSON.test &&
        databaseJSON.test.database &&
        databaseJSON.test.password &&
        databaseJSON.test.user
    );
    if ( !isValidDatabaseJSON ) {
        throw new Error("database.json should be object like are " + JSON.stringify({
            test: {
                host: "localhost",
                port: 5432,
                database: "test-database",
                "user": "test-user",
                "password": "test-password"
            }
        }, null, 4));
    }
    dbConfig = {
        ...dbConfig,
        ...databaseJSON.test
    }

    return dbConfig;
}