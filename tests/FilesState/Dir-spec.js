"use strict";

const assert = require("assert");
const fs = require("fs");
const FilesState = require("../../lib/FilesState");
const del = require("del");
const {expect, use} = require("chai");
const chaiShallowDeepEqualPlugin = require("chai-shallow-deep-equal");

use(chaiShallowDeepEqualPlugin);

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

describe("FilesState watch create and remove folders", () => {
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

    
    it("remove empty dir", async() => {

        let dirPath = ROOT_TMP_PATH + "/some-dir";

        fs.mkdirSync(dirPath);

        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);

        let counter = 0;
        filesState.on("change", () => {
            counter++;
        });

        watchers_to_stop.push(filesState);
        await filesState.watch();


        del.sync(dirPath);
        await sleep(50);

        assert.equal(counter, 0);
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });

    it("create empty dir", async() => {

        let dirPath = ROOT_TMP_PATH + "/some-dir";

        let filesState = FilesState.create({
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
        
        let dirPath = ROOT_TMP_PATH + "/some-dir.sql";

        let filesState = FilesState.create({
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
        
        let dirPath = ROOT_TMP_PATH + "/some-dir.sql";
        fs.mkdirSync(dirPath);


        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);

        let counter = 0;
        filesState.on("change", () => {
            counter++;
        });

        watchers_to_stop.push(filesState);
        await filesState.watch();

        del.sync(dirPath);
        await sleep(50);

        assert.equal(counter, 0);
        expect(filesState.getFunctions()).to.be.shallowDeepEqual([]);
    });


    it("create dir and create file", async() => {
        
        let dirPath = ROOT_TMP_PATH + "/some-dir";
        let filePath = dirPath + "/some.sql";

        let filesState = FilesState.create({
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
        
        let dirPath = ROOT_TMP_PATH + "/some-dir";
        let filePath = dirPath + "/some.sql";

        fs.mkdirSync(dirPath);
        fs.writeFileSync(filePath, test_func1_sql);

        let filesState = FilesState.create({
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

        del.sync(dirPath);
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