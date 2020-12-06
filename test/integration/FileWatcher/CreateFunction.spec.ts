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

describe("integration/FileWatcher watch create functions", () => {
    
    const ROOT_TMP_PATH = __dirname + "/tmp";
    const {watch} = watcher(ROOT_TMP_PATH);
    
    it("create function", async() => {
        
        const filePath = ROOT_TMP_PATH + "/create-func.sql";
        
        let migration!: Migration;
        let counter = 0;

        const fsWatcher = await watch((_migration) => {
            migration = _migration;
            counter++;
        });

        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        await sleep(50);
        
        assert.equal(counter, 1);
        
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
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
    });
        
    it("expected error on duplicate functions", async() => {
        const filePath1 = ROOT_TMP_PATH + "/create-func1.sql";
        const filePath2 = ROOT_TMP_PATH + "/create-func2.sql";
        
        
        const fsWatcher = await watch();

        let error: Error | undefined;
        fsWatcher.on("error", (err) => {
            error = err;
        });

        fs.writeFileSync(filePath1, TEST_FUNC1_SQL);
        await sleep(50);

        fs.writeFileSync(filePath2, TEST_FUNC1_SQL);
        await sleep(50);

        
        assert.equal(error && error.message, "duplicate function public.test_func1()");

        expect( flatMap(fsWatcher.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
    });

    it("create md file", async() => {
        
        const filePath = ROOT_TMP_PATH + "/create-func.md";
        
        let counter = 0;
        const fsWatcher = await watch(() => {
            counter++;
        });

        
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        await sleep(50);
        
        assert.strictEqual(counter, 0);
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions)).to.be.shallowDeepEqual([]);
    });


    it("twice create function", async() => {
        
        const filePath1 = ROOT_TMP_PATH + "/func1.sql";
        const filePath2 = ROOT_TMP_PATH + "/func2.sql";

        
        let migration!: Migration;
        let counter = 0;

        const fsWatcher = await watch((_migration) => {
            migration = _migration;
            counter++;
        });


        fs.writeFileSync(filePath1, TEST_FUNC1_SQL);
        await sleep(50);
        
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
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);


        fs.writeFileSync(filePath2, TEST_FUNC2_SQL);
        await sleep(50);
        
        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [],
                triggers: []
            },
            toCreate: {
                functions: [
                    TEST_FUNC2
                ],
                triggers: []
            }
        });
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            TEST_FUNC1,
            TEST_FUNC2
        ]);
    });

    it("create function with comment", async() => {
        
        const filePath = ROOT_TMP_PATH + "/create-func.sql";
        
        let migration!: Migration;
        let counter = 0;

        const fsWatcher = await watch((_migration) => {
            migration = _migration;
            counter++;
        });
        
        fs.writeFileSync(filePath, TEST_FUNC1_SQL + `
            comment on function test_func1() is 'sweet'
        `);
        await sleep(50);
        
        assert.equal(counter, 1);
        
        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [],
                triggers: []
            },
            toCreate: {
                functions: [
                    {...TEST_FUNC1, comment: "sweet"}
                ],
                triggers: []
            }
        });
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions)).to.be.shallowDeepEqual([
            {...TEST_FUNC1, comment: "sweet"}
        ]);
    });
});