import fs from "fs";
import fse from "fs-extra";
import { FilesState } from "../../lib/FilesState";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { generateEmptyFunction, VOID_BODY } from "../utils/generateEmptyFunction";

use(chaiShallowDeepEqualPlugin);

describe("FilesState parse files in sub dirs", () => {
    const ROOT_TMP_PATH = __dirname + "/tmp";
        
    beforeEach(() => {
        if ( fs.existsSync(ROOT_TMP_PATH) ) {
            fse.removeSync(ROOT_TMP_PATH);
        }
        fs.mkdirSync(ROOT_TMP_PATH);
    });
    
    afterEach(() => {
        fse.removeSync(ROOT_TMP_PATH);
    });

    

    // we want parsing only *.sql files
    it("parse folder with one sql file and one md file", () => {
        // create sql file
        const sql = generateEmptyFunction("some_func");
        fs.writeFileSync(ROOT_TMP_PATH + "/some.sql", sql);

        // create md file,
        fs.writeFileSync(ROOT_TMP_PATH + "/some.md", sql);

        // parse folder
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        // check
        const func = {
            language: "plpgsql",
            schema: "public",
            name: "some_func",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        expect(filesState.getFiles()).to.be.shallowDeepEqual([
            {
                name: "some.sql",
                folder: filesState.folders[0],
                path: "some.sql",
                content: {
                    functions: [func]
                }
            }
        ]);

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
            func
        ]);
    });

    
    it("parse folder with two sql files", () => {

        // create first sql file
        const sql1 = generateEmptyFunction("some_func_1");
        fs.writeFileSync(ROOT_TMP_PATH + "/some1.sql", sql1);

        // create second sql file
        const sql2 = generateEmptyFunction("some_func_2");
        fs.writeFileSync(ROOT_TMP_PATH + "/some2.sql", sql2);



        // parse folder
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        const func1 = {
            language: "plpgsql",
            schema: "public",
            name: "some_func_1",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        const func2 = {
            language: "plpgsql",
            schema: "public",
            name: "some_func_2",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        // check
        expect(filesState.getFiles()).to.be.shallowDeepEqual([
            {
                name: "some1.sql",
                path: "some1.sql",
                folder: filesState.folders[0],
                content: {
                    functions: [func1]
                }
            },
            {
                name: "some2.sql",
                path: "some2.sql",
                folder: filesState.folders[0],
                content: {
                    functions: [func2]
                }
            }
        ]);

        expect(filesState.getFunctions()).to.be.shallowDeepEqual([
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
        const filesState = FilesState.create({
            folder: ROOT_TMP_PATH
        });

        let files = filesState.getFiles();
        
        // for test stability we sorting result
        files = files.sort((file1, file2) => 
            file1.content.functions[0].name > file2.content.functions[0].name ?
                1 : -1
        );

        // check
        expect(files).to.be.shallowDeepEqual([
            {
                name: "test1.sql",
                path: "first/test1.sql",
                folder: filesState.folders[0],
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
                folder: filesState.folders[0],
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
                folder: filesState.folders[0],
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
                folder: filesState.folders[0],
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
                folder: filesState.folders[0],
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