"use strict";

const assert = require("assert");
const fs = require("fs");
const FilesState = require("../../lib/FilesState");
const del = require("del");

const VOID_BODY = `begin
end`;
function generateEmptyFunction(name) {
    return `
        create or replace function ${name}()
        returns void as $body$${VOID_BODY}$body$
        language plpgsql;
    `.trim();
}

describe("FilesState watch for many directories", () => {
    const ROOT_TMP_PATH = __dirname + "/tmp";
        
    beforeEach(() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            del.sync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });
    
    afterEach(() => {
        del.sync(ROOT_TMP_PATH);
    });

    
    it("read folders", () => {
        // create root dirs
        fs.mkdirSync(ROOT_TMP_PATH + "/root_1");
        fs.mkdirSync(ROOT_TMP_PATH + "/root_2");

        // create sql files
        let sql1 = generateEmptyFunction("root_1");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_1/some.sql", sql1);

        let sql2 = generateEmptyFunction("root_2");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", sql2);


        // parse folder
        let filesState = FilesState.create({
            folder: [
                ROOT_TMP_PATH + "/root_1",
                ROOT_TMP_PATH + "/root_2"
            ]
        });

        // check
        let func1 = {
            language: "plpgsql",
            schema: "public",
            name: "root_1",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };
        let func2 = {
            language: "plpgsql",
            schema: "public",
            name: "root_2",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        assert.deepEqual(filesState.getFiles(), [
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_1",
                content: {
                    functions: [func1]
                }
            },
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_2",
                content: {
                    functions: [func2]
                }
            }
        ]);

        assert.deepEqual(filesState.getFunctions(), [
            func1,
            func2
        ]);
    });


    it("watch many folders", async() => {
        // create root dirs
        fs.mkdirSync(ROOT_TMP_PATH + "/root_1");
        fs.mkdirSync(ROOT_TMP_PATH + "/root_2");


        // parse folder
        let filesState = FilesState.create({
            folder: [
                ROOT_TMP_PATH + "/root_1",
                ROOT_TMP_PATH + "/root_2"
            ]
        });
        
        await filesState.watch();
        

        // create sql files
        let sql1 = generateEmptyFunction("root_1");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_1/some.sql", sql1);

        let sql2 = generateEmptyFunction("root_2");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", sql2);

        await sleep(50);

        // check
        let func1 = {
            language: "plpgsql",
            schema: "public",
            name: "root_1",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };
        let func2 = {
            language: "plpgsql",
            schema: "public",
            name: "root_2",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        assert.deepEqual(filesState.getFiles(), [
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_1",
                content: {
                    functions: [func1]
                }
            },
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_2",
                content: {
                    functions: [func2]
                }
            }
        ]);

        assert.deepEqual(filesState.getFunctions(), [
            func1,
            func2
        ]);
    });


    it("watch deleting from some folder", async() => {
        // create root dirs
        fs.mkdirSync(ROOT_TMP_PATH + "/root_1");
        fs.mkdirSync(ROOT_TMP_PATH + "/root_2");

        // create sql files
        let sql1 = generateEmptyFunction("root_1");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_1/some.sql", sql1);

        let sql2 = generateEmptyFunction("root_2");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", sql2);


        // parse folder
        let filesState = FilesState.create({
            folder: [
                ROOT_TMP_PATH + "/root_1",
                ROOT_TMP_PATH + "/root_2"
            ]
        });
        
        await filesState.watch();
        

        fs.unlinkSync(ROOT_TMP_PATH + "/root_2/some.sql");
        await sleep(50);

        // check
        let func1 = {
            language: "plpgsql",
            schema: "public",
            name: "root_1",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        assert.deepEqual(filesState.getFiles(), [
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_1",
                content: {
                    functions: [func1]
                }
            }
        ]);

        assert.deepEqual(filesState.getFunctions(), [
            func1
        ]);
    });


    it("watch changes from some folder", async() => {
        // create root dirs
        fs.mkdirSync(ROOT_TMP_PATH + "/root_1");
        fs.mkdirSync(ROOT_TMP_PATH + "/root_2");

        // create sql files
        let sql1 = generateEmptyFunction("root_1");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_1/some.sql", sql1);

        let sql2 = generateEmptyFunction("root_2");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", sql2);


        // parse folder
        let filesState = FilesState.create({
            folder: [
                ROOT_TMP_PATH + "/root_1",
                ROOT_TMP_PATH + "/root_2"
            ]
        });
        
        await filesState.watch();
        

        let sql3 = generateEmptyFunction("changed_func");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", sql3);

        await sleep(50);

        // check
        let func1 = {
            language: "plpgsql",
            schema: "public",
            name: "root_1",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };
        let func2 = {
            language: "plpgsql",
            schema: "public",
            name: "changed_func",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        assert.deepEqual(filesState.getFiles(), [
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_1",
                content: {
                    functions: [func1]
                }
            },
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_2",
                content: {
                    functions: [func2]
                }
            }
        ]);

        assert.deepEqual(filesState.getFunctions(), [
            func1,
            func2
        ]);
    });
});


async function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}
