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
    returns: {type: "trigger"},
    body: {content: "select 1"}
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

const only_function_sql = `
    create or replace function nice()
    returns bigint as $$select 1$$
    language sql;
`;
const only_function = {
    language: "sql",
    schema: "public",
    name: "nice",
    args: [],
    returns: {type: "bigint"},
    body: {content: "select 1"}
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
    returns: {type: "trigger"},
    body: {content: "select 2"}
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


describe("FilesState watch change triggers", () => {
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

    
    it("change trigger", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-trigger.sql";
        fs.writeFileSync(filePath, test_func1_sql);
        

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger1
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
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [
                    test_func1
                ],
                triggers: [
                    test_trigger1
                ]
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
        assert.equal(counter, 1);
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func2
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger2
        ]);
    });


    it("write file same trigger, no changes", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-trigger.sql";
        fs.writeFileSync(filePath, test_func1_sql);
        

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger1
        ]);
        

        let counter = 0;
        filesState.on("change", () => {
            counter++;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        
        fs.writeFileSync(filePath, test_func1_sql);
        await sleep(50);
        
        assert.equal(counter, 0);
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger1
        ]);
    });

    it("expected error on duplicate triggers", async() => {
        const filePath1 = ROOT_TMP_PATH + "/change-trigger1.sql";
        const filePath2 = ROOT_TMP_PATH + "/change-trigger2.sql";
        fs.writeFileSync(filePath1, test_func1_sql);
        fs.writeFileSync(filePath2, test_func2_sql);
        

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1,
            test_func2
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger1,
            test_trigger2
        ]);
        

        let error: Error | undefined;
        filesState.on("error", (err) => {
            error = err;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        
        fs.writeFileSync(filePath2, `
            create or replace function another_func()
            returns trigger as $body$select 10$body$
            language sql;
        
            create trigger some_trigger
            before insert
            on operation.company
            for each row
            execute procedure another_func()
        `);
        await sleep(50);
        
        assert.equal(error && error.message, "duplicate trigger some_trigger on operation.company");

        expect( filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger1
        ]);
    });

    
    it("twice change function", async() => {

        const filePath = ROOT_TMP_PATH + "/change-trigger.sql";
        fs.writeFileSync(filePath, test_func1_sql);
        

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger1
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
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [
                    test_func1
                ],
                triggers: [
                    test_trigger1
                ]
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
        assert.equal(counter, 1);
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func2
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger2
        ]);



        fs.writeFileSync(filePath, test_func1_sql);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [
                    test_func2
                ],
                triggers: [
                    test_trigger2
                ]
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
        assert.equal(counter, 2);
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger1
        ]);
    });

    it("change trigger on function", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-trigger.sql";
        fs.writeFileSync(filePath, test_func1_sql);
        

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger1
        ]);
        

        let changes;
        let counter = 0;
        filesState.on("change", (_changes) => {
            changes = _changes;
            counter++;
        });
        watchers_to_stop.push(filesState);
        
        await filesState.watch();
        
        
        fs.writeFileSync(filePath, only_function_sql);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [
                    test_func1
                ],
                triggers: [
                    test_trigger1
                ]
            },
            create: {
                functions: [
                    only_function
                ],
                triggers: []
            }
        });
        assert.equal(counter, 1);
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            only_function
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([]);
    });
    
    it("change function on trigger", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-trigger.sql";
        fs.writeFileSync(filePath, only_function_sql);
        

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            only_function
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([]);
        

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
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [
                    only_function
                ],
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
        assert.equal(counter, 1);
        
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger1
        ]);
    });
});