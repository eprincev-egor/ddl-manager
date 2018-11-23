"use strict";

const assert = require("assert");
const fs = require("fs");
const DdlManager = require("../../lib/DdlManager");
const del = require("del");

const ROOT_TMP_PATH = __dirname + "/tmp";

before(() => {
    if ( !fs.existsSync(ROOT_TMP_PATH) ) {
        fs.mkdirSync(ROOT_TMP_PATH);
    }
});

const VOID_BODY = `begin
end`;
function generateEmptyFunction(name) {
    return `
        create or replace function ${name}()
        returns void as $body$${VOID_BODY}$body$
        language plpgsql;
    `.trim();
}

describe("DdlManager.parseFolder", () => {
    
    it("parse nonexistent folder", () => {

        try {
            DdlManager.parseFolder("---");
            
            assert.ok(false, "expected error for nonexistent folder");
        } catch(err) {
            assert.equal(err.message, "folder \"---\" not found");
        }
    });

    it("parse empty folder", () => {
        let folderPath = ROOT_TMP_PATH + "/empty";
    
        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);
        

        let result = DdlManager.parseFolder(folderPath);

        assert.deepEqual(result, []);

        // clear state
        del.sync(folderPath);
    });

    it("parse folder with one file", () => {
        let folderPath = ROOT_TMP_PATH + "/one-file";

        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);

        // create sql file
        let sql = generateEmptyFunction("some_func");
        fs.writeFileSync(folderPath + "/some.sql", sql);

        // parse folder
        let result = DdlManager.parseFolder(folderPath);

        // check
        assert.deepEqual(result, [
            {
                name: "some.sql",
                content: {
                    function: {
                        language: "plpgsql",
                        schema: "public",
                        name: "some_func",
                        args: [],
                        returns: "void",
                        body: VOID_BODY,
                        freeze: false
                    }
                }
            }
        ]);

        del.sync(folderPath);
    });

    // we want parsing only *.sql files
    it("parse folder with one sql file and one md file", () => {
        let folderPath = ROOT_TMP_PATH + "/one-file";

        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);

        // create sql file
        let sql = generateEmptyFunction("some_func");
        fs.writeFileSync(folderPath + "/some.sql", sql);

        // create md file,
        fs.writeFileSync(folderPath + "/some.md", sql);

        // parse folder
        let result = DdlManager.parseFolder(folderPath);

        // check
        assert.deepEqual(result, [
            {
                name: "some.sql",
                content: {
                    function: {
                        language: "plpgsql",
                        schema: "public",
                        name: "some_func",
                        args: [],
                        returns: "void",
                        body: VOID_BODY,
                        freeze: false
                    }
                }
            }
        ]);

        del.sync(folderPath);
    });

    it("parse folder with two sql files", () => {
        let folderPath = ROOT_TMP_PATH + "/two-files";

        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);


        // create first sql file
        let sql1 = generateEmptyFunction("some_func_1");
        fs.writeFileSync(folderPath + "/some1.sql", sql1);

        // create second sql file
        let sql2 = generateEmptyFunction("some_func_2");
        fs.writeFileSync(folderPath + "/some2.sql", sql2);



        // parse folder
        let result = DdlManager.parseFolder(folderPath);

        // check
        assert.deepEqual(result, [
            {
                name: "some1.sql",
                content: {
                    function: {
                        language: "plpgsql",
                        schema: "public",
                        name: "some_func_1",
                        args: [],
                        returns: "void",
                        body: VOID_BODY,
                        freeze: false
                    }
                }
            },
            {
                name: "some2.sql",
                content: {
                    function: {
                        language: "plpgsql",
                        schema: "public",
                        name: "some_func_2",
                        args: [],
                        returns: "void",
                        body: VOID_BODY,
                        freeze: false
                    }
                }
            }
        ]);

        del.sync(folderPath);
    });

    it("parse folder with sub folders", () => {
        let folderPath = ROOT_TMP_PATH + "/sub-test";

        // we want empty folder!
        if ( fs.existsSync(folderPath) ) {
            del.sync(folderPath);
        }
        fs.mkdirSync(folderPath);


        // first/test1.sql
        // first/x.sql
        // first/second/test2.sql
        // first/second/x.sql
        // first/second/third/test3.sql
        // first/second/third/x.sql
        
        fs.mkdirSync(folderPath + "/first");
        fs.writeFileSync(
            folderPath + "/first/test1.sql",
            generateEmptyFunction("test_1")
        );
        fs.writeFileSync(
            folderPath + "/first/x.sql",
            generateEmptyFunction("xx_1")
        );
        
        fs.mkdirSync(folderPath + "/first/second");
        fs.writeFileSync(
            folderPath + "/first/second/test2.sql", 
            generateEmptyFunction("test_2")
        );

        fs.mkdirSync(folderPath + "/first/second/third");
        fs.writeFileSync(
            folderPath + "/first/second/third/test3.sql", 
            generateEmptyFunction("test_3")
        );
        fs.writeFileSync(
            folderPath + "/first/second/third/x.sql",
            generateEmptyFunction("xx_3")
        );


        // parse folder
        let result = DdlManager.parseFolder(folderPath);
        
        // for test stability we sorting result
        result = result.sort((file1, file2) => 
            file1.content.function.name > file2.content.function.name ?
                1 : -1
        );

        // check
        assert.deepEqual(result, [
            {
                name: "test1.sql",
                content: {
                    function: {
                        language: "plpgsql",
                        schema: "public",
                        name: "test_1",
                        args: [],
                        returns: "void",
                        body: VOID_BODY,
                        freeze: false
                    }
                }
            },
            {
                name: "test2.sql",
                content: {
                    function: {
                        language: "plpgsql",
                        schema: "public",
                        name: "test_2",
                        args: [],
                        returns: "void",
                        body: VOID_BODY,
                        freeze: false
                    }
                }
            },
            {
                name: "test3.sql",
                content: {
                    function: {
                        language: "plpgsql",
                        schema: "public",
                        name: "test_3",
                        args: [],
                        returns: "void",
                        body: VOID_BODY,
                        freeze: false
                    }
                }
            },
            {
                name: "x.sql",
                content: {
                    function: {
                        language: "plpgsql",
                        schema: "public",
                        name: "xx_1",
                        args: [],
                        returns: "void",
                        body: VOID_BODY,
                        freeze: false
                    }
                }
            },
            {
                name: "x.sql",
                content: {
                    function: {
                        language: "plpgsql",
                        schema: "public",
                        name: "xx_3",
                        args: [],
                        returns: "void",
                        body: VOID_BODY,
                        freeze: false
                    }
                }
            }
        ]);



        del.sync(folderPath);
    });

});