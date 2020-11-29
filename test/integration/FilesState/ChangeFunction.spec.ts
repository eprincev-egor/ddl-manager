import assert from "assert";
import fs from "fs";
import fse from "fs-extra";
import { flatMap } from "lodash";
import { FileReader } from "../../../lib/fs/FileReader";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { sleep } from "../sleep";

use(chaiShallowDeepEqualPlugin);

const WATCHERS_TO_STOP: any[] = [];

const TEST_FUNC1_SQL = `
    create or replace function some_func1()
    returns void as $body$select 1$body$
    language sql;
`;
const TEST_FUNC1 = {
    language: "sql",
    schema: "public",
    name: "some_func1",
    args: [],
    returns: {type: "void"},
    body: "select 1"
};
const TEST_FUNC2_SQL = `
    create or replace function some_func2()
    returns void as $body$select 2$body$
    language sql;
`;
const TEST_FUNC2 = {
    language: "sql",
    schema: "public",
    name: "some_func2",
    args: [],
    returns: {type: "void"},
    body: "select 2"
};

describe("integration/FilesState watch change functions", () => {
    const ROOT_TMP_PATH = __dirname + "/tmp";
    
    beforeEach(() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });
    
    afterEach(() => {
        fse.removeSync(ROOT_TMP_PATH);

        WATCHERS_TO_STOP.forEach(filesReader => 
            filesReader.stopWatch()
        );
    });

    
    it("change function", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-func.sql";
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        

        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
        

        let changes;
        let counter = 0;
        filesReader.on("change", (_changes) => {
            changes = _changes;
            counter++;
        });
        WATCHERS_TO_STOP.push(filesReader);
        
        await filesReader.watch();
        
        
        fs.writeFileSync(filePath, TEST_FUNC2_SQL);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [
                    TEST_FUNC1
                ],
                triggers: []
            },
            create: {
                functions: [
                    TEST_FUNC2
                ],
                triggers: []
            }
        });
        assert.equal(counter, 1);
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC2
        ]);
    });


    it("write file same function, no changes", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-func.sql";
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        

        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
        

        let counter = 0;
        filesReader.on("change", () => {
            counter++;
        });
        WATCHERS_TO_STOP.push(filesReader);
        
        await filesReader.watch();
        
        
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        await sleep(50);
        
        assert.equal(counter, 0);
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
    });

    it("expected error on duplicate functions", async() => {
        const filePath1 = ROOT_TMP_PATH + "/change-func1.sql";
        const filePath2 = ROOT_TMP_PATH + "/change-func2.sql";
        fs.writeFileSync(filePath1, TEST_FUNC1_SQL);
        fs.writeFileSync(filePath2, TEST_FUNC2_SQL);
        

        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1,
            TEST_FUNC2
        ]);
        

        let error: Error | undefined;
        filesReader.on("error", (err) => {
            error = err;
        });
        WATCHERS_TO_STOP.push(filesReader);
        
        await filesReader.watch();
        
        
        fs.writeFileSync(filePath2, TEST_FUNC1_SQL);
        await sleep(50);
        
        assert.equal(error && error.message, "duplicate function public.some_func1()");

        expect( flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
    });


    it("twice change function", async() => {

        const filePath = ROOT_TMP_PATH + "/change-func.sql";
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        

        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
        

        let changes;
        let counter = 0;
        filesReader.on("change", (_changes) => {
            changes = _changes;
            counter++;
        });
        WATCHERS_TO_STOP.push(filesReader);
        
        await filesReader.watch();
        
        
        fs.writeFileSync(filePath, TEST_FUNC2_SQL);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [
                    TEST_FUNC1
                ],
                triggers: []
            },
            create: {
                functions: [
                    TEST_FUNC2
                ],
                triggers: []
            }
        });
        assert.equal(counter, 1);
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC2
        ]);



        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [
                    TEST_FUNC2
                ],
                triggers: []
            },
            create: {
                functions: [
                    TEST_FUNC1
                ],
                triggers: []
            }
        });
        assert.equal(counter, 2);
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
    });

    it("change comment on function", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-func.sql";
        fs.writeFileSync(filePath, TEST_FUNC1_SQL + `
            comment on function some_func1() is 'nice'
        `);
        

        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            {...TEST_FUNC1, comment: "nice"}
        ]);

        let changes;
        let counter = 0;
        filesReader.on("change", (_changes) => {
            changes = _changes;
            counter++;
        });
        WATCHERS_TO_STOP.push(filesReader);
        
        await filesReader.watch();
        
        
        fs.writeFileSync(filePath, TEST_FUNC1_SQL + `
            comment on function some_func1() is 'good'
        `);
        await sleep(50);
        
        expect(changes).to.be.shallowDeepEqual({
            drop: {
                functions: [
                    TEST_FUNC1
                ],
                triggers: []
            },
            create: {
                functions: [
                    {...TEST_FUNC1, comment: "good"}
                ],
                triggers: []
            }
        });
        assert.equal(counter, 1);
        
        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            {...TEST_FUNC1, comment: "good"}
        ]);
    });
});