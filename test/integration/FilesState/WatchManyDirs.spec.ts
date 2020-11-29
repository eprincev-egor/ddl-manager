import fs from "fs";
import fse from "fs-extra";
import { FileReader } from "../../../lib/fs/FileReader";
import { flatMap } from "lodash";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { sleep } from "../sleep";
import { generateEmptyFunction, VOID_BODY } from "./fixture/generateEmptyFunction";

use(chaiShallowDeepEqualPlugin);

describe("integration/FilesState watch for many directories", () => {
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

    
    it("read folders", () => {
        // create root dirs
        fs.mkdirSync(ROOT_TMP_PATH + "/root_1");
        fs.mkdirSync(ROOT_TMP_PATH + "/root_2");

        // create sql files
        const sql1 = generateEmptyFunction("root_1");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_1/some.sql", sql1);

        const sql2 = generateEmptyFunction("root_2");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", sql2);


        // parse folder
        const filesReader = FileReader.read({
            folder: [
                ROOT_TMP_PATH + "/root_1",
                ROOT_TMP_PATH + "/root_2"
            ]
        });

        // check
        const func1 = {
            language: "plpgsql",
            schema: "public",
            name: "root_1",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };
        const func2 = {
            language: "plpgsql",
            schema: "public",
            name: "root_2",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        expect(filesReader.state.files).to.be.shallowDeepEqual([
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

        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            func1,
            func2
        ]);
    });


    it("watch many folders", async() => {
        // create root dirs
        fs.mkdirSync(ROOT_TMP_PATH + "/root_1");
        fs.mkdirSync(ROOT_TMP_PATH + "/root_2");


        // parse folder
        const filesReader = FileReader.read({
            folder: [
                ROOT_TMP_PATH + "/root_1",
                ROOT_TMP_PATH + "/root_2"
            ]
        });
        
        await filesReader.watch();
        

        // create sql files
        const sql1 = generateEmptyFunction("root_1");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_1/some.sql", sql1);

        const sql2 = generateEmptyFunction("root_2");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", sql2);

        await sleep(50);

        // check
        const func1 = {
            language: "plpgsql",
            schema: "public",
            name: "root_1",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };
        const func2 = {
            language: "plpgsql",
            schema: "public",
            name: "root_2",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        expect(filesReader.state.files).to.be.shallowDeepEqual([
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

        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            func1,
            func2
        ]);
    });


    it("watch deleting from some folder", async() => {
        // create root dirs
        fs.mkdirSync(ROOT_TMP_PATH + "/root_1");
        fs.mkdirSync(ROOT_TMP_PATH + "/root_2");

        // create sql files
        const sql1 = generateEmptyFunction("root_1");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_1/some.sql", sql1);

        const sql2 = generateEmptyFunction("root_2");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", sql2);


        // parse folder
        const filesReader = FileReader.read({
            folder: [
                ROOT_TMP_PATH + "/root_1",
                ROOT_TMP_PATH + "/root_2"
            ]
        });
        
        await filesReader.watch();
        

        fs.unlinkSync(ROOT_TMP_PATH + "/root_2/some.sql");
        await sleep(50);

        // check
        const func1 = {
            language: "plpgsql",
            schema: "public",
            name: "root_1",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        expect(filesReader.state.files).to.be.shallowDeepEqual([
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_1",
                content: {
                    functions: [func1]
                }
            }
        ]);

        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            func1
        ]);
    });


    it("watch changes from some folder", async() => {
        // create root dirs
        fs.mkdirSync(ROOT_TMP_PATH + "/root_1");
        fs.mkdirSync(ROOT_TMP_PATH + "/root_2");

        // create sql files
        const sql1 = generateEmptyFunction("root_1");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_1/some.sql", sql1);

        const sql2 = generateEmptyFunction("root_2");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", sql2);


        // parse folder
        const filesReader = FileReader.read({
            folder: [
                ROOT_TMP_PATH + "/root_1",
                ROOT_TMP_PATH + "/root_2"
            ]
        });
        
        await filesReader.watch();
        

        const sql3 = generateEmptyFunction("changed_func");
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", sql3);

        await sleep(50);

        // check
        const func1 = {
            language: "plpgsql",
            schema: "public",
            name: "root_1",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };
        const func2 = {
            language: "plpgsql",
            schema: "public",
            name: "changed_func",
            args: [],
            returns: {type: "void"},
            body: VOID_BODY
        };

        expect(filesReader.state.files).to.be.shallowDeepEqual([
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

        expect(flatMap(filesReader.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            func1,
            func2
        ]);
    });
});
