import { DDLManager } from "../../../../lib/DDLManager";
import { PostgresDriver } from "../../../../lib/database/PostgresDriver";
import { CacheScanner } from "../../../../lib/Auditor/CacheScanner";
import { FileReader } from "../../../../lib/fs/FileReader";
import { Database } from "../../../../lib/database/schema/Database";
import { CacheColumnGraph } from "../../../../lib/Comparator/graph/CacheColumnGraph";
import { getDBClient } from "../../getDbClient";
import fs from "fs";
import fse from "fs-extra";
import { Pool } from "pg";

export const ROOT_TMP_PATH = __dirname + "/tmp";

export async function prepare() {
    const db = await getDBClient();

    await db.query(`
        drop schema public cascade;
        create schema public;

        create table companies (
            id serial primary key,
            name text,
            note text
        );
        create table orders (
            id serial primary key,
            id_client integer,
            doc_number text,
            profit integer,
            note text
        );

        insert into companies (name) 
        values ('client'), ('partner');
        insert into orders (id_client, doc_number)
        values
            (1, 'order-1'),
            (1, 'order-2'),
            (2, 'order-3');
    `);

    if ( fs.existsSync(ROOT_TMP_PATH) ) {
        fse.removeSync(ROOT_TMP_PATH);
    }
    fs.mkdirSync(ROOT_TMP_PATH);

    return db;
}

export async function buildDDL(db: Pool) {
    await DDLManager.build({
        db,
        folder: ROOT_TMP_PATH,
        throwError: true,
        needLogs: false
    });
}

export function createScanner(db: Pool) {
    const fsState = FileReader.read([ROOT_TMP_PATH]);
    const dbState = new Database();

    const scanner = new CacheScanner(
        new PostgresDriver(db),
        dbState,
        CacheColumnGraph.build(
            new Database().aggregators,
            fsState.allCache()
        )
    );
    return scanner;
}