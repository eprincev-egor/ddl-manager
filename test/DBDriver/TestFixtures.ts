import {readDatabaseOptions} from "../utils";
import { DBDriver } from "../../lib/db/DBDriver";
import pg from "pg";
import {PgDBDriver} from "../../lib/db/PgDBDriver";
import fs from "fs";
import path from "path";
import assert from "assert";
import { BaseDBObjectModel } from "../../lib/objects/base-layers/BaseDBObjectModel";

export interface ITestFixturesParams {
    fixturesPath: string;
    load: (pgDriver: PgDBDriver) => 
            Promise<BaseDBObjectModel<any>[]>;
    prepareDDL?: (ddl: string) => string;
    prepareDBO?: (dbo: BaseDBObjectModel<any>["TJson"]) => void;
}

export class TestFixtures
implements ITestFixturesParams {
    private dbConfig: DBDriver["options"];
    private db: pg.Client;

    protected pgDriver: PgDBDriver;

    fixturesPath: string;
    load: (pgDriver: PgDBDriver) => 
        Promise<BaseDBObjectModel<any>[]>;
    prepareDDL?: (ddl: string) => string;
    prepareDBO?: (dbo: BaseDBObjectModel<any>) => void;
    
    constructor(params: ITestFixturesParams) {
        this.fixturesPath = params.fixturesPath;
        this.load = params.load;
        this.prepareDDL = params.prepareDDL;
        this.prepareDBO = params.prepareDBO;
        this.dbConfig = readDatabaseOptions();
    }

    async before() {
        this.db = new pg.Client(this.dbConfig);
        await this.db.connect();
    }

    async beforeEach() {
        this.pgDriver = new PgDBDriver(this.dbConfig);
        await this.pgDriver.connect();
    }

    async afterEach() {
        await this.pgDriver.end();
    }

    async after() {
        this.db.end();
    }

    testFixtures() {
        const fixtures = fs.readdirSync(this.fixturesPath);

        for (const dirName of fixtures) {
            const dirPath = path.join(this.fixturesPath, dirName);

            const ddlPath = path.join(dirPath, "ddl.sql");
            let ddl = fs.readFileSync(ddlPath).toString();
            if ( this.prepareDDL ) {
                ddl = this.prepareDDL(ddl);
            }

            const resultPath = path.join(dirPath, "result");
            const expectedJSON = require(resultPath);

            it(dirName, async() => {

                await this.db.query(`
                    drop schema public cascade;
                    create schema public;
                    ${ddl}
                `);
                
                const objects = await this.load(this.pgDriver);

                const actualJSON = objects.map(dboModel => dboModel.toJSON());
                if ( this.prepareDBO ) {
                    actualJSON.forEach(this.prepareDBO);
                }

                assert.deepStrictEqual(
                    actualJSON,
                    expectedJSON
                );
            });
        }
    }
}