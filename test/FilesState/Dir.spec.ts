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
    body: "select 1"
};

describe("FilesState watch create and remove folders", () => {
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

    
    it("remove empty dir", async() => {

        const dirPath = ROOT_TMP_PATH + "/some-dir";

        fs.mkdirSync(dirPath);

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);

        let counter = 0;
        filesState.on("change", () => {
            counter++;
        });

        watchers_to_stop.push(filesState);
        await filesState.watch();


        fse.removeSync(dirPath);
        await sleep(50);

        assert.equal(counter, 0);
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });

    it("create empty dir", async() => {

        const dirPath = ROOT_TMP_PATH + "/some-dir";

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);

        let counter = 0;
        filesState.on("change", () => {
            counter++;
        });

        watchers_to_stop.push(filesState);
        await filesState.watch();


        fs.mkdirSync(dirPath);
        await sleep(50);

        assert.equal(counter, 0);
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });

    it("create dir.sql", async() => {
        
        const dirPath = ROOT_TMP_PATH + "/some-dir.sql";

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);

        let counter = 0;
        filesState.on("change", () => {
            counter++;
        });

        watchers_to_stop.push(filesState);
        await filesState.watch();


        fs.mkdirSync(dirPath);
        await sleep(50);

        assert.equal(counter, 0);
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });

    it("remove dir.sql", async() => {
        
        const dirPath = ROOT_TMP_PATH + "/some-dir.sql";
        fs.mkdirSync(dirPath);


        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);

        let counter = 0;
        filesState.on("change", () => {
            counter++;
        });

        watchers_to_stop.push(filesState);
        await filesState.watch();

        fse.removeSync(dirPath);
        await sleep(50);

        assert.equal(counter, 0);
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });


    it("create dir and create file", async() => {
        
        const dirPath = ROOT_TMP_PATH + "/some-dir";
        const filePath = dirPath + "/some.sql";

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);

        let counter = 0;
        let changes;
        filesState.on("change", (_changes) => {
            counter++;
            changes = _changes;
        });

        watchers_to_stop.push(filesState);
        await filesState.watch();


        fs.mkdirSync(dirPath);
        await sleep(50);

        fs.writeFileSync(filePath, test_func1_sql);
        await sleep(50);

        assert.equal(counter, 1);
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);

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
    });


    it("remove dir with file", async() => {
        
        const dirPath = ROOT_TMP_PATH + "/some-dir";
        const filePath = dirPath + "/some.sql";

        fs.mkdirSync(dirPath);
        fs.writeFileSync(filePath, test_func1_sql);

        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            test_func1
        ]);


        let counter = 0;
        let changes;
        filesState.on("change", (_changes) => {
            counter++;
            changes = _changes;
        });

        watchers_to_stop.push(filesState);
        await filesState.watch();

        fse.removeSync(dirPath);
        await sleep(50);

        assert.equal(counter, 1);
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);

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
    });

});