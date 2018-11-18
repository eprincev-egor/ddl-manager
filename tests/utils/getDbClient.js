"use strict";

const pg = require("pg");

async function getDBClient() {
    let dbConfig = {
        database: false,
        user: false,
        password: false,
        host: "localhost",
        port: 5432
    };
    
    let dbConfigFromFile;
    try {
        dbConfigFromFile = require("../../database-config");
    } catch(err) {
        throw new Error("Создайте файл database-config.js в корне. Модуль должен возвращать объект, имеющий свойства database, user, password, host, port");
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

    if ( !dbConfig.user && !dbConfig.password && !dbConfig.database ) {
        throw new Error("Настройки подключения к базе не переданы!");
    }

    let dbClient;
    try {
        dbClient = new pg.Client(dbConfig);
        await dbClient.connect();
    } catch(err) {
        throw new Error(
            "Не удалось подключится к базе данных с настройками: " + 
            JSON.stringify(dbConfig, null, 4) + 
            "\n" +
            "\n" +
            err.message
        );
    }

    return dbClient;
}

module.exports = getDBClient;