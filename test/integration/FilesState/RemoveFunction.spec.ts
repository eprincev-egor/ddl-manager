import assert from "assert";
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

describe("integration/FilesState watch remove functions", () => {
    const ROOT_TMP_PATH = __dirname + "/tmp";
    
    beforeEach(() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });
    
    afterEach(() => {
        watchers_to_stop.forEach(filesState => 
            filesState.stopWatch()
        );

        fse.removeSync(ROOT_TMP_PATH);
    });

    
    it("remove function", async() => {
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";

        fs.writeFileSync(filePath, test_func1_sql);

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
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

        expect(changes).to.be.shallowDeepEqual({
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

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });


    it("remove .md file", async() => {
        
        fs.writeFileSync(ROOT_TMP_PATH + "/test.sql", test_func1_sql);
        
        const mdFilePath = ROOT_TMP_PATH + "/test.md";
        fs.writeFileSync(mdFilePath, test_func1_sql);

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        // content from test.sql
        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
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

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([test_func1]);
    });

    it("remove file from sub dir", async() => {
        const filePath = ROOT_TMP_PATH + "/child/xxx.sql";
        
        fs.mkdirSync(ROOT_TMP_PATH + "/child");
        fs.writeFileSync(filePath, test_func1_sql);
        
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        // content from test.sql
        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
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

        expect(changes).to.be.shallowDeepEqual({
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

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });

    it("twice remove", async() => {
        const filePath1 = ROOT_TMP_PATH + "/file1.sql";
        const filePath2 = ROOT_TMP_PATH + "/file2.sql";

        fs.writeFileSync(filePath1, test_func1_sql);
        fs.writeFileSync(filePath2, test_func2_sql);
        
        
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        // content from test.sql
        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
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

        expect(changes).to.be.shallowDeepEqual({
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


        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func2
        ]);

        fs.unlinkSync(filePath2);

        await sleep(50);

        expect(changes).to.be.shallowDeepEqual({
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


        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });

    it("remove function with comments", async() => {
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";

        fs.writeFileSync(filePath, test_func1_sql +  `
            comment on function some_func1() is 'awesome'
        `);

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            {...test_func1, comment: "awesome"}
        ]);

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
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });


    it("remove function with default arg", async() => {
        const func_sql = `
            create or replace function some_func1(x integer default null)
            returns void as $body$begin\n\nend$body$
            language plpgsql;
        `;
        const func = {
            language: "plpgsql",
            schema: "public",
            name: "some_func1",
            args: [{
                name: "x",
                type: "integer",
                default: "null"
            }],
            returns: {type: "void"},
            body: "begin\n\nend"
        };

        const filePath = ROOT_TMP_PATH + "/test-file.sql";

        fs.writeFileSync(filePath, func_sql);

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual(
            [func]
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
                    func
                ],
                triggers: []
            },
            create: {
                functions: [],
                triggers: []
            }
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });

});