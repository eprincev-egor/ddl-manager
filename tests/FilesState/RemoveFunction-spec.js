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

describe("FilesState watch remove functions", () => {
    const ROOT_TMP_PATH = __dirname + "/tmp";
    
    beforeEach(() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            del.sync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });
    
    afterEach(() => {
        watchers_to_stop.forEach(filesState => 
            filesState.stopWatch()
        );

        del.sync(ROOT_TMP_PATH);
    });

    
    it("remove function", async() => {
        
        let filePath = ROOT_TMP_PATH + "/test-file.sql";

        fs.writeFileSync(filePath, test_func1_sql);

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        assert.deepEqual(
            filesState.getFunctions(), 
            [test_func1]
        );

        let changes;
        filesState.on("change", (_changes) => {
            changes = _changes;
        });
        watchers_to_stop.push(filesState);

        await filesState.watch();

        fs.unlinkSync(filePath);
        
        await sleep(50);

        assert.deepEqual(changes, {
            drop: {
                functions: [
                    test_func1
                ],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        });

        assert.deepEqual(filesState.getFunctions(), []);
    });


    it("remove .md file", async() => {
        
        fs.writeFileSync(ROOT_TMP_PATH + "/test.sql", test_func1_sql);
        
        let mdFilePath = ROOT_TMP_PATH + "/test.md";
        fs.writeFileSync(mdFilePath, test_func1_sql);

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        // content from test.sql
        assert.deepEqual(
            filesState.getFunctions(), 
            [test_func1]
        );

        let hasChanges = false;
        filesState.on("change", () => {
            hasChanges = true;
        });
        watchers_to_stop.push(filesState);

        await filesState.watch();

        // remove .MD file
        fs.unlinkSync(mdFilePath);
        
        await sleep(100);

        assert.strictEqual(hasChanges, false);

        assert.deepEqual(filesState.getFunctions(), [test_func1]);
    });

    it("remove file from sub dir", async() => {
        let filePath = ROOT_TMP_PATH + "/child/xxx.sql";
        
        fs.mkdirSync(ROOT_TMP_PATH + "/child");
        fs.writeFileSync(filePath, test_func1_sql);
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        // content from test.sql
        assert.deepEqual(
            filesState.getFunctions(), 
            [test_func1]
        );


        let changes;
        filesState.on("change", (_changes) => {
            changes = _changes;
        });
        watchers_to_stop.push(filesState);


        await filesState.watch();
        
        fs.unlinkSync(filePath);

        await sleep(50);

        assert.deepEqual(changes, {
            drop: {
                functions: [
                    test_func1
                ],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        });

        assert.deepEqual(filesState.getFunctions(), []);
    });

    it("twice remove", async() => {
        let filePath1 = ROOT_TMP_PATH + "/file1.sql";
        let filePath2 = ROOT_TMP_PATH + "/file2.sql";

        fs.writeFileSync(filePath1, test_func1_sql);
        fs.writeFileSync(filePath2, test_func2_sql);
        
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        // content from test.sql
        assert.deepEqual(
            filesState.getFunctions(), 
            [
                test_func1,
                test_func2
            ]
        );


        let changes;
        filesState.on("change", (_changes) => {
            changes = _changes;
        });
        watchers_to_stop.push(filesState);


        await filesState.watch();

        fs.unlinkSync(filePath1);

        await sleep(50);

        assert.deepEqual(changes, {
            drop: {
                functions: [
                    test_func1
                ],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        });


        assert.deepEqual(filesState.getFunctions(), [
            test_func2
        ]);

        fs.unlinkSync(filePath2);

        await sleep(50);

        assert.deepEqual(changes, {
            drop: {
                functions: [
                    test_func2
                ],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        });


        assert.deepEqual(filesState.getFunctions(), []);
    });


});