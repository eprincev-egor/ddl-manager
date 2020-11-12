import fs from "fs";
import fse from "fs-extra";
import { FilesState } from "../../../lib/FilesState";
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

describe("integration/FilesState watch remove triggers", () => {
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

    
    it("remove trigger", async() => {
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";

        fs.writeFileSync(filePath, test_func1_sql);

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
            [test_func1]
        );

        expect(filesState.getTriggers()).to.be.shallowDeepEqual(
            [test_trigger1]
        );

        let changes;
        filesState.on("change", (_changes) => {
            changes = _changes;
        });
        watchers_to_stop.push(filesState);

        await filesState.watch();

        fs.unlinkSync(filePath);
        
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
                functions: [],
                triggers: []
            }
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([]);
    });


    it("twice remove", async() => {
        const filePath1 = ROOT_TMP_PATH + "/file1.sql";
        const filePath2 = ROOT_TMP_PATH + "/file2.sql";

        fs.writeFileSync(filePath1, test_func1_sql);
        fs.writeFileSync(filePath2, test_func2_sql);
        
        
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
            [
                test_func1,
                test_func2
            ]
        );
        expect(filesState.getTriggers()).to.be.shallowDeepEqual(
            [
                test_trigger1,
                test_trigger2
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
                functions: [],
                triggers: []
            }
        });


        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func2
        ]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([
            test_trigger2
        ]);

        fs.unlinkSync(filePath2);

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
                functions: [],
                triggers: []
            }
        });


        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
        expect(filesState.getTriggers()).to.be.shallowDeepEqual([]);
    });
});