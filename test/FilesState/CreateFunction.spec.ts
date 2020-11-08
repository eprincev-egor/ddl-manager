import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { FilesState } from "../../lib/FilesState";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { sleep } from "../utils/sleep";

use(chaiShallowDeepEqualPlugin);

const watchers_to_stop: any[] = [];

const test_func1_sql = `
    create or replace function some_func1()
    returns void as $body$select 1$body$
    language sql;
`;
const test_func1 = {
    language: "sql",
    schema: "public",
    name: "some_func1",
    args: [],
    returns: {type: "void"},
    body: {content: "select 1"}
};
const test_func2_sql = `
    create or replace function some_func2()
    returns void as $body$select 2$body$
    language sql;
`;
const test_func2 = {
    language: "sql",
    schema: "public",
    name: "some_func2",
    args: [],
    returns: {type: "void"},
    body: {content: "select 2"}
};


describe("FilesState watch create functions", () => {
    const ROOT_TMP_PATH = __dirname + "/tmp";
    
    beforeEach(() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });
    
    afterEach(() => {
        fse.removeSync(ROOT_TMP_PATH);

        watchers_to_stop.forEach(filesState => 
            filesState.stopWatch()
        );
    });

    
    it("create function", async() => {
        
        const filePath = ROOT_TMP_PATH + "/create-func.sql";
        
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
            []
        );
        
        let changes;
        let counter = 0;
        filesState.on("change", (_changes) => {
            changes = _changes;
            counter++;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        fs.writeFileSync(filePath, test_func1_sql);
        
        await sleep(50);
        
        assert.equal(counter, 1);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    test_func1
                ],
                triggers: []
            }
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);
    });
        
    it("expected error on duplicate functions", async() => {
        const filePath1 = ROOT_TMP_PATH + "/create-func1.sql";
        const filePath2 = ROOT_TMP_PATH + "/create-func2.sql";
        
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        watchers_to_stop.push(filesState);
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
            []
        );

        let error: Error | undefined;
        filesState.on("error", (err) => {
            error = err;
        });

        await filesState.watch();
        
        fs.writeFileSync(filePath1, test_func1_sql);
        await sleep(50);

        fs.writeFileSync(filePath2, test_func1_sql);
        await sleep(50);

        
        assert.equal(error && error.message, "duplicate function public.some_func1()");

        expect( filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);
    });

    it("create md file", async() => {
        
        const filePath = ROOT_TMP_PATH + "/create-func.md";
        
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
            []
        );
        
        let hasChanges = false;
        filesState.on("change", () => {
            hasChanges = true;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        fs.writeFileSync(filePath, test_func1_sql);
        
        await sleep(50);
        
        assert.strictEqual(hasChanges, false);
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });


    it("twice create function", async() => {
        
        const filePath1 = ROOT_TMP_PATH + "/func1.sql";
        const filePath2 = ROOT_TMP_PATH + "/func2.sql";
        
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
            []
        );
        
        let changes;
        filesState.on("change", (_changes) => {
            changes = _changes;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        


        fs.writeFileSync(filePath1, test_func1_sql);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    test_func1
                ],
                triggers: []
            }
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);


        fs.writeFileSync(filePath2, test_func2_sql);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    test_func2
                ],
                triggers: []
            }
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1,
            test_func2
        ]);
    });

    it("create function with comment", async() => {
        
        const filePath = ROOT_TMP_PATH + "/create-func.sql";
        
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
            []
        );
        
        let changes;
        let counter = 0;
        filesState.on("change", (_changes) => {
            changes = _changes;
            counter++;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        fs.writeFileSync(filePath, test_func1_sql + `
            comment on function some_func1() is 'sweet'
        `);
        
        await sleep(50);
        
        assert.equal(counter, 1);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    {...test_func1, comment: "sweet"}
                ],
                triggers: []
            }
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            {...test_func1, comment: "sweet"}
        ]);
    });
});