import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { flatMap } from "lodash";
import { FileReader } from "../../../lib/fs/FileReader";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { sleep } from "../sleep";

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
    returns: {type: "trigger"},
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


describe("integration/FilesState watch create functions", () => {
    const ROOT_TMP_PATH = __dirname + "/tmp";
    
    beforeEach(() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });
    
    afterEach(() => {
        fse.removeSync(ROOT_TMP_PATH);

        watchers_to_stop.forEach(filesReader => 
            filesReader.stopWatch()
        );
    });

    
    it("create trigger", async() => {
        
        const filePath = ROOT_TMP_PATH + "/some_trigger.sql";
        
        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual(
            []
        );
        expect(flatMap(filesReader.state.files, file => file.content.triggers)).to.be.shallowDeepEqual(
            []
        );


        
        let changes;
        filesReader.on("change", (_changes) => {
            changes = _changes;
        });
        watchers_to_stop.push(filesReader);
        
        await filesReader.watch();
        
        fs.writeFileSync(filePath, test_func1_sql);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            toDrop: {
                functions: [],
                triggers: []
            },
            toCreate: {
                functions: [
                    test_func1
                ],
                triggers: [
                    test_trigger1
                ]
            }
        });
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(flatMap(filesReader.state.files, file => file.content.triggers)).to.be.shallowDeepEqual([
            test_trigger1
        ]);
    });

    it("expected error on duplicate triggers", async() => {
        const filePath1 = ROOT_TMP_PATH + "/create-trigger1.sql";
        const filePath2 = ROOT_TMP_PATH + "/create-trigger2.sql";
        
        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });
        watchers_to_stop.push(filesReader);
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual(
            []
        );
        expect(flatMap(filesReader.state.files, file => file.content.triggers)).to.be.shallowDeepEqual(
            []
        );


        let error: Error | undefined;
        filesReader.on("error", (err) => {
            error = err;
        });

        await filesReader.watch();
        
        fs.writeFileSync(filePath1, test_func1_sql);
        await sleep(50);

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
        await sleep(50);

        
        assert.equal(error && error.message, "duplicate trigger some_trigger on operation.company");

        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(flatMap(filesReader.state.files, file => file.content.triggers)).to.be.shallowDeepEqual([
            test_trigger1
        ]);
    });

    
    it("twice create trigger", async() => {
        
        const filePath1 = ROOT_TMP_PATH + "/trigger1.sql";
        const filePath2 = ROOT_TMP_PATH + "/trigger2.sql";
        
        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });
        watchers_to_stop.push(filesReader);
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual(
            []
        );
        expect(flatMap(filesReader.state.files, file => file.content.triggers)).to.be.shallowDeepEqual(
            []
        );


        
        let changes;
        filesReader.on("change", (_changes) => {
            changes = _changes;
        });
        
        await filesReader.watch();
        


        fs.writeFileSync(filePath1, test_func1_sql);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            toDrop: {
                functions: [],
                triggers: []
            },
            toCreate: {
                functions: [
                    test_func1
                ],
                triggers: [
                    test_trigger1
                ]
            }
        });
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            test_func1
        ]);
        expect(flatMap(filesReader.state.files, file => file.content.triggers)).to.be.shallowDeepEqual([
            test_trigger1
        ]);


        fs.writeFileSync(filePath2, test_func2_sql);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            toDrop: {
                functions: [],
                triggers: []
            },
            toCreate: {
                functions: [
                    test_func2
                ],
                triggers: [
                    test_trigger2
                ]
            }
        });
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            test_func1,
            test_func2
        ]);
        expect(flatMap(filesReader.state.files, file => file.content.triggers)).to.be.shallowDeepEqual([
            test_trigger1,
            test_trigger2
        ]);
    });
    
});