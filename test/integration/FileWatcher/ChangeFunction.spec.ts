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

describe("integration/FileWatcher watch change functions", () => {
    
    const ROOT_TMP_PATH = __dirname + "/tmp";
    const {watch} = watcher(ROOT_TMP_PATH);
    
    it("change function", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-func.sql";
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        
        let migration!: Migration;
        let counter = 0;

        const fsWatcher = await watch((_migration) => {
            migration = _migration;
            counter++;
        });

        fs.writeFileSync(filePath, TEST_FUNC2_SQL);
        await sleep(50);
        
        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [
                    TEST_FUNC1
                ],
                triggers: []
            },
            toCreate: {
                functions: [
                    TEST_FUNC2
                ],
                triggers: []
            }
        });
        assert.equal(counter, 1);
        
        expect(fsWatcher.state.allNotHelperFunctions())
            .to.be.shallowDeepEqual([
                TEST_FUNC2
            ]);
    });


    it("write file same function, no changes", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-func.sql";
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        

        let counter = 0;
        const fsWatcher = await watch(() => {
            counter++;
        });
        
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        await sleep(150);
        
        assert.equal(counter, 0);
        
        expect(fsWatcher.state.allNotHelperFunctions())
            .to.be.shallowDeepEqual([
                TEST_FUNC1
            ]);
    });

    it("expected error on duplicated functions", async() => {
        const filePath1 = ROOT_TMP_PATH + "/change-func1.sql";
        const filePath2 = ROOT_TMP_PATH + "/change-func2.sql";
        fs.writeFileSync(filePath1, TEST_FUNC1_SQL);
        fs.writeFileSync(filePath2, TEST_FUNC2_SQL);
        
        const fsWatcher = await watch();

        let error: Error | undefined;
        fsWatcher.on("error", (err) => {
            error = err;
        });
        
        fs.writeFileSync(filePath2, TEST_FUNC1_SQL);
        await sleep(50);
        
        assert.equal(error && error.message, "duplicated function public.test_func1()");

        expect( fsWatcher.state.allNotHelperFunctions() ).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
    });


    it("twice change function", async() => {

        const filePath = ROOT_TMP_PATH + "/change-func.sql";
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        

        let migration!: Migration;
        let counter = 0;

        const fsWatcher = await watch((_migration) => {
            migration = _migration;
            counter++;
        });
        
        
        fs.writeFileSync(filePath, TEST_FUNC2_SQL);
        await sleep(50);
        
        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [
                    TEST_FUNC1
                ],
                triggers: []
            },
            toCreate: {
                functions: [
                    TEST_FUNC2
                ],
                triggers: []
            }
        });
        assert.equal(counter, 1);
        
        expect(fsWatcher.state.allNotHelperFunctions()).to.be.shallowDeepEqual([
            TEST_FUNC2
        ]);



        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        await sleep(50);
        
        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [
                    TEST_FUNC2
                ],
                triggers: []
            },
            toCreate: {
                functions: [
                    TEST_FUNC1
                ],
                triggers: []
            }
        });
        assert.equal(counter, 2);
        
        expect(fsWatcher.state.allNotHelperFunctions())
            .to.be.shallowDeepEqual([
                TEST_FUNC1
            ]);
    });

});