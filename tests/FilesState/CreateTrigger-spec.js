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
    returns trigger as $body$select 1$body$
    language sql;

    create trigger some_trigger
    before insert
    on operation.company
    for each row
    execute procedure some_func1()
`;
const test_func1 = {
    language: "sql",
    schema: "public",
    name: "some_func1",
    args: [],
    returns: "trigger",
    body: "select 1"
};
const test_trigger1 = {
    table: {
        schema: "operation",
        name: "company"
    },
    name: "some_trigger",
    before: true,
    insert: true,
    procedure: {
        schema: "public",
        name: "some_func1"
    }
};


const test_func2_sql = `
    create or replace function some_func2()
    returns trigger as $body$select 2$body$
    language sql;

    create trigger some_trigger2
    before delete
    on operation.company
    for each row
    execute procedure some_func2()
`;
const test_func2 = {
    language: "sql",
    schema: "public",
    name: "some_func2",
    args: [],
    returns: "trigger",
    body: "select 2"
};
const test_trigger2 = {
    table: {
        schema: "operation",
        name: "company"
    },
    name: "some_trigger2",
    before: true,
    delete: true,
    procedure: {
        schema: "public",
        name: "some_func2"
    }
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

    
    it("create trigger", async() => {
        
        let filePath = ROOT_TMP_PATH + "/some_trigger.sql";
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        assert.deepEqual(
            filesState.getFunctions(), 
            []
        );
        assert.deepEqual(
            filesState.getTriggers(), 
            []
        );


        
        let changes;
        filesState.on("change", (_changes) => {
            changes = _changes;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        fs.writeFileSync(filePath, test_func1_sql);
        await sleep(250);
        
        assert.deepEqual(changes, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    test_func1
                ],
                triggers: [
                    test_trigger1
                ]
            }
        });
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);
        assert.deepEqual(filesState.getTriggers(), [
            test_trigger1
        ]);
    });

    it("expected error on duplicate triggers", async() => {
        let filePath1 = ROOT_TMP_PATH + "/create-trigger1.sql";
        let filePath2 = ROOT_TMP_PATH + "/create-trigger2.sql";
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        watchers_to_stop.push(filesState);
        
        assert.deepEqual(
            filesState.getFunctions(), 
            []
        );
        assert.deepEqual(
            filesState.getTriggers(), 
            []
        );


        let error;
        filesState.on("error", (err) => {
            error = err;
        });

        await filesState.watch();
        
        fs.writeFileSync(filePath1, test_func1_sql);
        await sleep(250);

        fs.writeFileSync(filePath2, `
            create or replace function another_func()
            returns trigger as $body$select 1$body$
            language sql;
        
            create trigger some_trigger
            before delete
            on operation.company
            for each row
            execute procedure another_func()
        `);
        await sleep(250);

        
        assert.equal(error && error.message, "duplicate trigger some_trigger on operation.company");

        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);
        assert.deepEqual(filesState.getTriggers(), [
            test_trigger1
        ]);
    });

    
    it("twice create trigger", async() => {
        
        let filePath1 = ROOT_TMP_PATH + "/trigger1.sql";
        let filePath2 = ROOT_TMP_PATH + "/trigger2.sql";
        
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        watchers_to_stop.push(filesState);
        
        assert.deepEqual(
            filesState.getFunctions(), 
            []
        );
        assert.deepEqual(
            filesState.getTriggers(), 
            []
        );


        
        let changes;
        filesState.on("change", (_changes) => {
            changes = _changes;
        });
        
        await filesState.watch();
        


        fs.writeFileSync(filePath1, test_func1_sql);
        await sleep(250);
        
        assert.deepEqual(changes, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    test_func1
                ],
                triggers: [
                    test_trigger1
                ]
            }
        });
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1
        ]);
        assert.deepEqual(filesState.getTriggers(), [
            test_trigger1
        ]);


        fs.writeFileSync(filePath2, test_func2_sql);
        await sleep(250);
        
        assert.deepEqual(changes, {
            drop: {
                functions: [],
                triggers: []
            },
            create: {
                functions: [
                    test_func2
                ],
                triggers: [
                    test_trigger2
                ]
            }
        });
        
        assert.deepEqual(filesState.getFunctions(), [
            test_func1,
            test_func2
        ]);
        assert.deepEqual(filesState.getTriggers(), [
            test_trigger1,
            test_trigger2
        ]);
    });
    
});