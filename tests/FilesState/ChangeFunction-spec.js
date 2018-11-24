"use strict";

const assert = require("assert");
const fs = require("fs");
const FilesState = require("../../lib/FilesState");
const del = require("del");

async function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

const watchers_to_stop = [];

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
    returns: "void",
    body: "select 1"
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
    returns: "void",
    body: "select 2"
};

describe("FilesState watch change functions", () => {
    const ROOT_TMP_PATH = __dirname + "/tmp";
    
    beforeEach(() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            del.sync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });
    
    afterEach(() => {
        del.sync(ROOT_TMP_PATH);

        watchers_to_stop.forEach(filesState => 
            filesState.stopWatch()
        );
    });

    
    it("change function", async() => {
        
        let filePath = ROOT_TMP_PATH + "/change-func.sql";
        fs.writeFileSync(filePath, test_func1_sql);
        

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);
        

        let changes;
        let counter = 0;
        filesState.on("change", (_changes) => {
            changes = _changes;
            counter++;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        
        fs.writeFileSync(filePath, test_func2_sql);
        await sleep(250);
        
        assert.deepEqual(changes, {
            drop: {
                functions: [
                    test_func1
                ],
                triggers: []
            },
            create: {
                functions: [
                    test_func2
                ],
                triggers: []
            }
        });
        assert.equal(counter, 1);
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func2
        ]);
    });


    it("write file same function, no changes", async() => {
        
        let filePath = ROOT_TMP_PATH + "/change-func.sql";
        fs.writeFileSync(filePath, test_func1_sql);
        

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);
        

        let counter = 0;
        filesState.on("change", () => {
            counter++;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        
        fs.writeFileSync(filePath, test_func1_sql);
        await sleep(250);
        
        assert.equal(counter, 0);
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);
    });

    it("expected error on duplicate functions", async() => {
        let filePath1 = ROOT_TMP_PATH + "/change-func1.sql";
        let filePath2 = ROOT_TMP_PATH + "/change-func2.sql";
        fs.writeFileSync(filePath1, test_func1_sql);
        fs.writeFileSync(filePath2, test_func2_sql);
        

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1,
            test_func2
        ]);
        

        let error;
        filesState.on("error", (err) => {
            error = err;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        
        fs.writeFileSync(filePath2, test_func1_sql);
        await sleep(250);
        
        assert.equal(error && error.message, "duplicate function public.some_func1()");

        assert.deepEqual( filesState.getFunctions(), [
            test_func1
        ]);
    });


    it("twice change function", async() => {

        let filePath = ROOT_TMP_PATH + "/change-func.sql";
        fs.writeFileSync(filePath, test_func1_sql);
        

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);
        

        let changes;
        let counter = 0;
        filesState.on("change", (_changes) => {
            changes = _changes;
            counter++;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        
        fs.writeFileSync(filePath, test_func2_sql);
        await sleep(250);
        
        assert.deepEqual(changes, {
            drop: {
                functions: [
                    test_func1
                ],
                triggers: []
            },
            create: {
                functions: [
                    test_func2
                ],
                triggers: []
            }
        });
        assert.equal(counter, 1);
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func2
        ]);



        fs.writeFileSync(filePath, test_func1_sql);
        await sleep(250);
        
        assert.deepEqual(changes, {
            drop: {
                functions: [
                    test_func2
                ],
                triggers: []
            },
            create: {
                functions: [
                    test_func1
                ],
                triggers: []
            }
        });
        assert.equal(counter, 2);
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);
    });
});