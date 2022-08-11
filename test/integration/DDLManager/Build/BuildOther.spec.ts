import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { getDBClient } from "../../getDbClient";
import { DDLManager } from "../../../../lib/DDLManager";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";

use(chaiShallowDeepEqualPlugin);

const ROOT_TMP_PATH = __dirname + "/tmp";

describe("integration/DDLManager.build other tests", () => {
    let db: any;
    const dbConfig = require("../../../../ddl-manager-config");
    
    beforeEach(async() => {
        db = await getDBClient();

        await db.query(`
            drop schema public cascade;
            create schema public;
        `);

        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });

    afterEach(async() => {
        await db.end();
    });
    
    it("build nonexistent folder", async() => {
        try {
            await DDLManager.build({
                db, 
                folder: "---"
            });
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("build empty folder", async() => {
        const folderPath = ROOT_TMP_PATH + "/empty";
        fs.mkdirSync(folderPath);

        await DDLManager.build({
            db, 
            folder: folderPath
        });

        // expected build without errors
        assert.ok(true);
    });

    it("build with dbConfig", async() => {
        const folderPath = ROOT_TMP_PATH + "/simple-func";
        fs.mkdirSync(folderPath);

        fs.writeFileSync(folderPath + "/nice.sql", `
            create or replace function nice()
            returns integer as $body$
                begin
                    return 1;
                end
            $body$
            language plpgsql;
        `);

        await DDLManager.build({
            db: {
                database: dbConfig.database,
                user: dbConfig.user,
                password: dbConfig.password,
                host: dbConfig.host,
                port: dbConfig.port
            }, 
            folder: folderPath
        });

        const result = await db.query("select nice() as nice");
        const row = result.rows[0];

        expect(row).to.be.shallowDeepEqual({
            nice: 1
        });
    });

    it("build from many folders", async() => {

        const folderPath1 = ROOT_TMP_PATH + "/many-folder-1";
        fs.mkdirSync(folderPath1);

        const folderPath2 = ROOT_TMP_PATH + "/many-folder-2";
        fs.mkdirSync(folderPath2);

        fs.writeFileSync(folderPath1 + "/func1.sql", `
            create or replace function func1()
            returns text as $body$
            begin
                return 'func1';
            end
            $body$
            language plpgsql;
        `);
        fs.writeFileSync(folderPath2 + "/func2.sql", `
            create or replace function func2()
            returns text as $body$
            begin
                return 'func2';
            end
            $body$
            language plpgsql;
        `);

        await DDLManager.build({
            db, 
            folder: [
                folderPath1, 
                folderPath2
            ]
        });


        const result = await db.query(`
            select 
                func1() as func1,
                func2() as func2
        `);
        expect(result.rows[0]).to.be.shallowDeepEqual({
            func1: "func1",
            func2: "func2"
        });
    });

});