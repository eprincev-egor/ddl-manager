import assert from "assert";
import fs from "fs";
import { flatMap } from "lodash";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { sleep } from "../sleep";
import { Migration } from "../../../lib/Migrator/Migration";
import { watcher } from "./utils/watcher";
import {
    TEST_FUNC1_SQL,
    TEST_FUNC1,
    TEST_FUNC2_SQL,
    TEST_FUNC2
} from "../fixture/functions";

use(chaiShallowDeepEqualPlugin);

describe("integration/FileWatcher watch remove functions", () => {
    
    const ROOT_TMP_PATH = __dirname + "/tmp";
    const {watch} = watcher(ROOT_TMP_PATH);
    
    it("remove function", async() => {
        
        const filePath = ROOT_TMP_PATH + "/test-file.sql";

        fs.writeFileSync(filePath, TEST_FUNC1_SQL);

        let migration!: Migration;
        const fsWatcher = await watch((_migration) => {
            migration = _migration;
        });


        fs.unlinkSync(filePath);
        await sleep(50);

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

        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.deep.equal([]);
    });


    it("remove .md file", async() => {
        
        fs.writeFileSync(ROOT_TMP_PATH + "/test.sql", TEST_FUNC1_SQL);
        
        const mdFilePath = ROOT_TMP_PATH + "/test.md";
        fs.writeFileSync(mdFilePath, TEST_FUNC1_SQL);

        let counter = 0;
        const fsWatcher = await watch((_migration) => {
            counter++;
        });

        // remove .MD file
        fs.unlinkSync(mdFilePath);
        await sleep(100);

        assert.strictEqual(counter, 0);

        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([TEST_FUNC1]);
    });

    it("remove file from sub dir", async() => {
        const filePath = ROOT_TMP_PATH + "/child/xxx.sql";
        
        fs.mkdirSync(ROOT_TMP_PATH + "/child");
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        
        let migration!: Migration;
        const fsWatcher = await watch((_migration) => {
            migration = _migration;
        });
        

        fs.unlinkSync(filePath);
        await sleep(50);

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

        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.deep.equal([]);
    });

    it("twice remove", async() => {
        const filePath1 = ROOT_TMP_PATH + "/file1.sql";
        const filePath2 = ROOT_TMP_PATH + "/file2.sql";

        fs.writeFileSync(filePath1, TEST_FUNC1_SQL);
        fs.writeFileSync(filePath2, TEST_FUNC2_SQL);
        
        
        let migration!: Migration;
        const fsWatcher = await watch((_migration) => {
            migration = _migration;
        });


        fs.unlinkSync(filePath1);
        await sleep(50);

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


        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([
                TEST_FUNC2
            ]);

        fs.unlinkSync(filePath2);
        await sleep(50);

        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [
                    TEST_FUNC2
                ],
                triggers: []
            },
            toCreate: {
                functions: [],
                triggers: []
            }
        });


        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.deep.equal([]);
    });

    it("remove function with default arg", async() => {
        const FUNC_SQL = `
            create or replace function test_func1(x integer default null)
            returns void as $body$begin\n\nend$body$
            language plpgsql;
        `;
        const FUNC = {
            language: "plpgsql",
            schema: "public",
            name: "test_func1",
            args: [{
                name: "x",
                type: "integer",
                default: "null"
            }],
            returns: {type: "void"},
            body: "begin\n\nend"
        };

        const filePath = ROOT_TMP_PATH + "/test-file.sql";

        fs.writeFileSync(filePath, FUNC_SQL);

        let migration!: Migration;
        const fsWatcher = await watch((_migration) => {
            migration = _migration;
        });

        fs.unlinkSync(filePath);
        await sleep(50);

        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [
                    FUNC
                ],
                triggers: []
            },
            toCreate: {
                functions: [],
                triggers: []
            }
        });

        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.deep.equal([]);
    });

});