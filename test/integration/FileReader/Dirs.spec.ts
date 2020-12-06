import fs from "fs";
import { flatMap } from "lodash";
import { FileReader } from "../../../lib/fs/FileReader";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { prepare } from "../utils/prepare";
import {
    TEST_FUNC1_SQL,
    TEST_FUNC2_SQL,
    TEST_FUNC1,
    TEST_FUNC2,
    VOID_FUNC1_SQL,
    VOID_FUNC2_SQL,
    VOID_FUNC1,
    VOID_FUNC2,
    TEST_FUNC3_SQL,
    TEST_FUNC3
} from "../fixture/functions";

use(chaiShallowDeepEqualPlugin);

describe("integration/FileReader read dirs", () => {

    const ROOT_TMP_PATH = __dirname + "/tmp";
    prepare(ROOT_TMP_PATH);

    it("read many root folders", () => {
        // create root dirs
        fs.mkdirSync(ROOT_TMP_PATH + "/root_1");
        fs.mkdirSync(ROOT_TMP_PATH + "/root_2");

        // create sql files
        fs.writeFileSync(ROOT_TMP_PATH + "/root_1/some.sql", VOID_FUNC1_SQL);
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", VOID_FUNC2_SQL);

        // parse folder
        const fsWatcher = FileReader.read({
            folder: [
                ROOT_TMP_PATH + "/root_1",
                ROOT_TMP_PATH + "/root_2"
            ]
        });

        // check
        expect(fsWatcher.state.files).to.be.shallowDeepEqual([
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_1",
                content: {
                    functions: [VOID_FUNC1]
                }
            },
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_2",
                content: {
                    functions: [VOID_FUNC2]
                }
            }
        ]);
    });

    // we want parsing only *.sql files
    it("parse folder with one sql file and one md file", () => {
        // create sql file
        fs.writeFileSync(ROOT_TMP_PATH + "/some.sql", VOID_FUNC1_SQL);

        // create md file,
        fs.writeFileSync(ROOT_TMP_PATH + "/some.md", VOID_FUNC1_SQL);

        // parse folder
        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });

        // check
        expect(filesReader.state.files).to.be.shallowDeepEqual([
            {
                name: "some.sql",
                folder: filesReader.rootFolders[0],
                path: "some.sql",
                content: {
                    functions: [VOID_FUNC1]
                }
            }
        ]);

        expect(flatMap(filesReader.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([
                VOID_FUNC1
            ]);
    });

    
    it("parse folder with two sql files", () => {

        // create first sql file
        fs.writeFileSync(ROOT_TMP_PATH + "/some1.sql", VOID_FUNC1_SQL);

        // create second sql file
        fs.writeFileSync(ROOT_TMP_PATH + "/some2.sql", VOID_FUNC2_SQL);



        // parse folder
        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });

        // check
        expect(filesReader.state.files).to.be.shallowDeepEqual([
            {
                name: "some1.sql",
                path: "some1.sql",
                folder: filesReader.rootFolders[0],
                content: {
                    functions: [VOID_FUNC1]
                }
            },
            {
                name: "some2.sql",
                path: "some2.sql",
                folder: filesReader.rootFolders[0],
                content: {
                    functions: [VOID_FUNC2]
                }
            }
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
            TEST_FUNC1_SQL
        );
        fs.writeFileSync(
            ROOT_TMP_PATH + "/first/void1.sql",
            VOID_FUNC1_SQL
        );
        
        fs.mkdirSync(ROOT_TMP_PATH + "/first/second");
        fs.writeFileSync(
            ROOT_TMP_PATH + "/first/second/test2.sql", 
            TEST_FUNC2_SQL
        );

        fs.mkdirSync(ROOT_TMP_PATH + "/first/second/third");
        fs.writeFileSync(
            ROOT_TMP_PATH + "/first/second/third/void2.sql", 
            VOID_FUNC2_SQL
        );
        fs.writeFileSync(
            ROOT_TMP_PATH + "/first/second/third/test3.sql",
            TEST_FUNC3_SQL
        );


        // parse folder
        const filesReader = FileReader.read({
            folder: ROOT_TMP_PATH
        });

        let files = filesReader.state.files;
        
        // for test stability we sorting result
        files = files.sort((file1, file2) => 
            file1.name > file2.name ?
                1 : -1
        );

        // check
        expect(files).to.be.shallowDeepEqual([
            {
                name: "test1.sql",
                path: "first/test1.sql",
                folder: filesReader.rootFolders[0],
                content: {
                    functions: [TEST_FUNC1]
                }
            },
            {
                name: "test2.sql",
                path: "first/second/test2.sql",
                folder: filesReader.rootFolders[0],
                content: {
                    functions: [TEST_FUNC2]
                }
            },
            {
                name: "test3.sql",
                path: "first/second/third/test3.sql",
                folder: filesReader.rootFolders[0],
                content: {
                    functions: [TEST_FUNC3]
                }
            },
            {
                name: "void1.sql",
                path: "first/void1.sql",
                folder: filesReader.rootFolders[0],
                content: {
                    functions: [VOID_FUNC1]
                }
            },
            {
                name: "void2.sql",
                path: "first/second/third/void2.sql",
                folder: filesReader.rootFolders[0],
                content: {
                    functions: [VOID_FUNC2]
                }
            }
        ]);
    });

});