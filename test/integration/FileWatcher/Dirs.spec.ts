import fs from "fs";
import fse from "fs-extra";
import { flatMap } from "lodash";
import assert from "assert";
import { FileReader } from "../../../lib/fs/FileReader";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { Migration } from "../../../lib/Migrator/Migration";
import { sleep } from "../sleep";
import {
    TEST_FUNC1_SQL,
    TEST_FUNC1,
    VOID_FUNC1_SQL,
    VOID_FUNC2_SQL,
    VOID_FUNC1,
    VOID_FUNC2
} from "../fixture/functions";
import { watcher } from "./utils/watcher";

use(chaiShallowDeepEqualPlugin);

describe("integration/FileWatcher watch for directories", () => {
    
    const ROOT_TMP_PATH = __dirname + "/tmp";
    const { watch } = watcher(ROOT_TMP_PATH);
    
    it("remove empty dir", async() => {

        const dirPath = ROOT_TMP_PATH + "/some-dir";
        fs.mkdirSync(dirPath);

        let counter = 0;
        const fsWatcher = await watch((_migration) => {
            counter++;
        });


        fse.removeSync(dirPath);
        await sleep(50);

        assert.equal(counter, 0);
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([]);
    });

    it("create empty dir", async() => {

        const dirPath = ROOT_TMP_PATH + "/some-dir";

        let counter = 0;
        const fsWatcher = await watch((_migration) => {
            counter++;
        });


        fs.mkdirSync(dirPath);
        await sleep(50);

        assert.equal(counter, 0);
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([]);
    });

    it("create dir.sql", async() => {
        
        const dirPath = ROOT_TMP_PATH + "/some-dir.sql";

        let counter = 0;
        const fsWatcher = await watch((_migration) => {
            counter++;
        });


        fs.mkdirSync(dirPath);
        await sleep(50);

        assert.equal(counter, 0);
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([]);
    });

    it("remove dir.sql", async() => {
        
        const dirPath = ROOT_TMP_PATH + "/some-dir.sql";
        fs.mkdirSync(dirPath);

        let counter = 0;
        const fsWatcher = await watch((_migration) => {
            counter++;
        });

        fse.removeSync(dirPath);
        await sleep(50);

        assert.equal(counter, 0);
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([]);
    });


    it("create dir and create file", async() => {
        
        const dirPath = ROOT_TMP_PATH + "/some-dir";
        const filePath = dirPath + "/some.sql";

        let migration!: Migration;
        let counter = 0;
        const fsWatcher = await watch((_migration) => {
            migration = _migration;
            counter++;
        });


        fs.mkdirSync(dirPath);
        await sleep(50);

        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        await sleep(50);

        assert.equal(counter, 1);
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([
                TEST_FUNC1
            ]);

        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [],
                triggers: []
            },
            toCreate: {
                functions: [
                    TEST_FUNC1
                ],
                triggers: []
            }
        });
    });


    it("remove dir with file", async() => {
        
        const dirPath = ROOT_TMP_PATH + "/some-dir";
        const filePath = dirPath + "/some.sql";

        fs.mkdirSync(dirPath);
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);

        let migration!: Migration;
        let counter = 0;
        const fsWatcher = await watch((_migration) => {
            migration = _migration;
            counter++;
        });


        fse.removeSync(dirPath);
        await sleep(50);

        assert.equal(counter, 1);
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([]);

        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [
                    TEST_FUNC1
                ],
                triggers: []
            },
            toCreate: {
                functions: [],
                triggers: []
            }
        });
    });


    it("watch many folders", async() => {
        // create root dirs
        fs.mkdirSync(ROOT_TMP_PATH + "/root_1");
        fs.mkdirSync(ROOT_TMP_PATH + "/root_2");


        // parse folder
        const fsWatcher = FileReader.read({
            folder: [
                ROOT_TMP_PATH + "/root_1",
                ROOT_TMP_PATH + "/root_2"
            ]
        });
        await fsWatcher.watch();

        // create sql files
        fs.writeFileSync(ROOT_TMP_PATH + "/root_1/some.sql", VOID_FUNC1_SQL);
        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", VOID_FUNC2_SQL);

        await sleep(50);

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


    it("watch deleting from some folder", async() => {
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
        await fsWatcher.watch();

        fs.unlinkSync(ROOT_TMP_PATH + "/root_2/some.sql");
        await sleep(50);

        // check
        expect(fsWatcher.state.files).to.be.shallowDeepEqual([
            {
                name: "some.sql",
                path: "some.sql",
                folder: ROOT_TMP_PATH + "/root_1",
                content: {
                    functions: [VOID_FUNC1]
                }
            }
        ]);
    });


    it("watch changes from some folder", async() => {
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
        await fsWatcher.watch();
        

        fs.writeFileSync(ROOT_TMP_PATH + "/root_2/some.sql", TEST_FUNC1_SQL);
        await sleep(50);

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
                    functions: [TEST_FUNC1]
                }
            }
        ]);
    });
});
