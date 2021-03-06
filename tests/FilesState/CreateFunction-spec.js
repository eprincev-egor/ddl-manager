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
    returns: {type: "void"},
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
    returns: {type: "void"},
    body: "select 2"
};


describe("FilesState watch create functions", () => {
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

    
    it("create function", async() => {
        
        let filePath = ROOT_TMP_PATH + "/create-func.sql";
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        assert.deepEqual(
            filesState.getFunctions(), 
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
        
        assert.deepEqual(changes, {
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
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);
    });
        
    it("expected error on duplicate functions", async() => {
        let filePath1 = ROOT_TMP_PATH + "/create-func1.sql";
        let filePath2 = ROOT_TMP_PATH + "/create-func2.sql";
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        watchers_to_stop.push(filesState);
        
        assert.deepEqual(
            filesState.getFunctions(), 
            []
        );

        let error;
        filesState.on("error", (err) => {
            error = err;
        });

        await filesState.watch();
        
        fs.writeFileSync(filePath1, test_func1_sql);
        await sleep(50);

        fs.writeFileSync(filePath2, test_func1_sql);
        await sleep(50);

        
        assert.equal(error && error.message, "duplicate function public.some_func1()");

        assert.deepEqual( filesState.getFunctions(), [
            test_func1
        ]);
    });

    it("create md file", async() => {
        
        let filePath = ROOT_TMP_PATH + "/create-func.md";
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        assert.deepEqual(
            filesState.getFunctions(), 
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
        
        assert.deepEqual(filesState.getFunctions(), []);
    });


    it("twice create function", async() => {
        
        let filePath1 = ROOT_TMP_PATH + "/func1.sql";
        let filePath2 = ROOT_TMP_PATH + "/func2.sql";
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        assert.deepEqual(
            filesState.getFunctions(), 
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
        
        assert.deepEqual(changes, {
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
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);


        fs.writeFileSync(filePath2, test_func2_sql);
        await sleep(50);
        
        assert.deepEqual(changes, {
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
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1,
            test_func2
        ]);
    });

    it("create function with comment", async() => {
        
        let filePath = ROOT_TMP_PATH + "/create-func.sql";
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        assert.deepEqual(
            filesState.getFunctions(), 
            []
        );
        
        assert.deepEqual(
            filesState.getComments(), 
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
        
        assert.deepEqual(changes, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    test_func1
                ],
                triggers: [],
                comments: [
                    {
                        function: {
                            schema: "public",
                            name: "some_func1",
                            args: []
                        },
                        comment: "sweet"
                    }
                ]
            }
        });
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);

        assert.deepEqual(filesState.getComments(), [
            {
                function: {
                    schema: "public",
                    name: "some_func1",
                    args: []
                },
                comment: "sweet"
            }
        ]);
    });
});