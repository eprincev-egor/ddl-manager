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

describe("FilesState parse files in sub dirs", () => {
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

    

    // we want parsing only *.sql files
    it("parse folder with one sql file and one md file", () => {
        // create sql file
        let sql = generateEmptyFunction("some_func");
        fs.writeFileSync(ROOT_TMP_PATH + "/some.sql", sql);

        // create md file,
        fs.writeFileSync(ROOT_TMP_PATH + "/some.md", sql);

        // parse folder
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        // check
        let func = {
            language: "plpgsql",
            schema: "public",
            name: "some_func",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        assert.deepEqual(filesState.getFiles(), [
            {
                name: "some.sql",
                path: "some.sql",
                content: {
                    functions: [func]
                }
            }
        ]);

        assert.deepEqual(filesState.getFunctions(), [
            func
        ]);
    });

    
    it("parse folder with two sql files", () => {

        // create first sql file
        let sql1 = generateEmptyFunction("some_func_1");
        fs.writeFileSync(ROOT_TMP_PATH + "/some1.sql", sql1);

        // create second sql file
        let sql2 = generateEmptyFunction("some_func_2");
        fs.writeFileSync(ROOT_TMP_PATH + "/some2.sql", sql2);



        // parse folder
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        let func1 = {
            language: "plpgsql",
            schema: "public",
            name: "some_func_1",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        let func2 = {
            language: "plpgsql",
            schema: "public",
            name: "some_func_2",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        // check
        assert.deepEqual(filesState.getFiles(), [
            {
                name: "some1.sql",
                path: "some1.sql",
                content: {
                    functions: [func1]
                }
            },
            {
                name: "some2.sql",
                path: "some2.sql",
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

    
    it("parse folder with sub folders", () => {

        // first/test1.sql
        // first/x.sql
        // first/second/test2.sql
        // first/second/x.sql
        // first/second/third/test3.sql
        // first/second/third/x.sql
        
        fs.mkdirSync(ROOT_TMP_PATH + "/first");
        fs.writeFileSync(
            ROOT_TMP_PATH + "/first/test1.sql",
            generateEmptyFunction("test_1")
        );
        fs.writeFileSync(
            ROOT_TMP_PATH + "/first/x.sql",
            generateEmptyFunction("xx_1")
        );
        
        fs.mkdirSync(ROOT_TMP_PATH + "/first/second");
        fs.writeFileSync(
            ROOT_TMP_PATH + "/first/second/test2.sql", 
            generateEmptyFunction("test_2")
        );

        fs.mkdirSync(ROOT_TMP_PATH + "/first/second/third");
        fs.writeFileSync(
            ROOT_TMP_PATH + "/first/second/third/test3.sql", 
            generateEmptyFunction("test_3")
        );
        fs.writeFileSync(
            ROOT_TMP_PATH + "/first/second/third/x.sql",
            generateEmptyFunction("xx_3")
        );


        // parse folder
        let filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        let files = filesState.getFiles();
        
        // for test stability we sorting result
        files = files.sort((file1, file2) => 
            file1.content.functions[0].name > file2.content.functions[0].name ?
                1 : -1
        );

        // check
        assert.deepEqual(files, [
            {
                name: "test1.sql",
                path: "first/test1.sql",
                content: {
                    functions: [{
                        language: "plpgsql",
                        schema: "public",
                        name: "test_1",
                        args: [],
                        returns: {type: "void"},
                        body: VOID_BODY
                    }]
                }
            },
            {
                name: "test2.sql",
                path: "first/second/test2.sql",
                content: {
                    functions: [{
                        language: "plpgsql",
                        schema: "public",
                        name: "test_2",
                        args: [],
                        returns: {type: "void"},
                        body: VOID_BODY
                    }]
                }
            },
            {
                name: "test3.sql",
                path: "first/second/third/test3.sql",
                content: {
                    functions: [{
                        language: "plpgsql",
                        schema: "public",
                        name: "test_3",
                        args: [],
                        returns: {type: "void"},
                        body: VOID_BODY
                    }]
                }
            },
            {
                name: "x.sql",
                path: "first/x.sql",
                content: {
                    functions: [{
                        language: "plpgsql",
                        schema: "public",
                        name: "xx_1",
                        args: [],
                        returns: {type: "void"},
                        body: VOID_BODY
                    }]
                }
            },
            {
                name: "x.sql",
                path: "first/second/third/x.sql",
                content: {
                    functions: [{
                        language: "plpgsql",
                        schema: "public",
                        name: "xx_3",
                        args: [],
                        returns: {type: "void"},
                        body: VOID_BODY
                    }]
                }
            }
        ]);
    });

});