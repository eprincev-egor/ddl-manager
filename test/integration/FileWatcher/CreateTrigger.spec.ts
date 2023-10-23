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
    TEST_TRIGGER1,
    TEST_FUNC2_SQL,
    TEST_FUNC2,
    TEST_TRIGGER2
} from "../fixture/triggers";

use(chaiShallowDeepEqualPlugin);

describe("integration/FileWatcher watch create functions", () => {
    
    const ROOT_TMP_PATH = __dirname + "/tmp";
    const {watch} = watcher(ROOT_TMP_PATH);
    
    it("create trigger", async() => {
        
        const filePath = ROOT_TMP_PATH + "/some_trigger.sql";
        
        let migration!: Migration;
        const fsWatcher = await watch((_migration) => {
            migration = _migration;
        });

        
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
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
                triggers: [
                    TEST_TRIGGER1
                ]
            }
        });
        
        expect(fsWatcher.state.allNotHelperFunctions()).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers)).to.be.shallowDeepEqual([
            TEST_TRIGGER1
        ]);
    });

    it("expected error on duplicated triggers", async() => {
        const filePath1 = ROOT_TMP_PATH + "/create-trigger1.sql";
        const filePath2 = ROOT_TMP_PATH + "/create-trigger2.sql";
        
        const fsWatcher = await watch();

        let error: Error | undefined;
        fsWatcher.on("error", (err) => {
            error = err;
        });

        
        fs.writeFileSync(filePath1, TEST_FUNC1_SQL);
        await sleep(50);

        fs.writeFileSync(filePath2, `
            create or replace function another_func()
            returns trigger as $body$select 1$body$
            language sql;
        
            create trigger some_trigger
            before delete
            on operation.company
            for each row
            execute procedure another_func()
        `);
        await sleep(50);

        
        assert.equal(error && error.message, "duplicated trigger some_trigger on operation.company");

        expect(fsWatcher.state.allNotHelperFunctions()).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers)).to.be.shallowDeepEqual([
            TEST_TRIGGER1
        ]);
    });

    
    it("twice create trigger", async() => {
        
        const filePath1 = ROOT_TMP_PATH + "/trigger1.sql";
        const filePath2 = ROOT_TMP_PATH + "/trigger2.sql";
        
        let migration!: Migration;
        const fsWatcher = await watch((_migration) => {
            migration = _migration;
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
                triggers: [
                    TEST_TRIGGER1
                ]
            }
        });
        
        expect(fsWatcher.state.allNotHelperFunctions()).to.be.shallowDeepEqual([
            TEST_FUNC1
        ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers)).to.be.shallowDeepEqual([
            TEST_TRIGGER1
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
                triggers: [
                    TEST_TRIGGER2
                ]
            }
        });
        
        expect(fsWatcher.state.allNotHelperFunctions()).to.be.shallowDeepEqual([
            TEST_FUNC1,
            TEST_FUNC2
        ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers)).to.be.shallowDeepEqual([
            TEST_TRIGGER1,
            TEST_TRIGGER2
        ]);
    });
    
});