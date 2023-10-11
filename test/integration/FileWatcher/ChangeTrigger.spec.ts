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
    ONLY_FUNCTION_SQL,
    ONLY_FUNCTION,
    TEST_FUNC2_SQL,
    TEST_FUNC2,
    TEST_TRIGGER2
} from "../fixture/triggers";

use(chaiShallowDeepEqualPlugin);


describe("integration/FileWatcher watch change triggers", () => {

    const ROOT_TMP_PATH = __dirname + "/tmp";
    const {watch} = watcher(ROOT_TMP_PATH);
    

    it("change trigger", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-trigger.sql";
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
                triggers: [
                    TEST_TRIGGER1
                ]
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
        assert.equal(counter, 1);
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([
                TEST_FUNC2
            ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers))
            .to.be.shallowDeepEqual([
                TEST_TRIGGER2
            ]);
    });


    it("write file same trigger, no changes", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-trigger.sql";
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        
        let counter = 0;
        const fsWatcher = await watch(() => {
            counter++;
        });
        
        
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        await sleep(50);
        
        assert.equal(counter, 0);
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([
                TEST_FUNC1
            ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers))
            .to.be.shallowDeepEqual([
                TEST_TRIGGER1
            ]);
    });

    it("expected error on duplicated triggers", async() => {
        const filePath1 = ROOT_TMP_PATH + "/change-trigger1.sql";
        const filePath2 = ROOT_TMP_PATH + "/change-trigger2.sql";
        fs.writeFileSync(filePath1, TEST_FUNC1_SQL);
        fs.writeFileSync(filePath2, TEST_FUNC2_SQL);
        

       
        const fsWatcher = await watch();

        let error: Error | undefined;
        fsWatcher.on("error", (err) => {
            error = err;
        });
        
        
        fs.writeFileSync(filePath2, `
            create or replace function another_func()
            returns trigger as $body$select 10$body$
            language sql;
        
            create trigger some_trigger
            before insert
            on operation.company
            for each row
            execute procedure another_func()
        `);
        await sleep(50);
        
        assert.equal(error && error.message, "duplicated trigger some_trigger on operation.company");

        expect( flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([
                TEST_FUNC1
            ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers))
            .to.be.shallowDeepEqual([
                TEST_TRIGGER1
            ]);
    });

    
    it("twice change function", async() => {

        const filePath = ROOT_TMP_PATH + "/change-trigger.sql";
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
                triggers: [
                    TEST_TRIGGER1
                ]
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
        assert.equal(counter, 1);
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([
                TEST_FUNC2
            ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers))
            .to.be.shallowDeepEqual([
                TEST_TRIGGER2
            ]);



        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        await sleep(50);
        
        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [
                    TEST_FUNC2
                ],
                triggers: [
                    TEST_TRIGGER2
                ]
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
        assert.equal(counter, 2);
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([
                TEST_FUNC1
            ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers))
            .to.be.shallowDeepEqual([
                TEST_TRIGGER1
            ]);
    });

    it("change trigger on function", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-trigger.sql";
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        

        
        let migration!: Migration;
        let counter = 0;

        const fsWatcher = await watch((_migration) => {
            migration = _migration;
            counter++;
        });
        
        
        fs.writeFileSync(filePath, ONLY_FUNCTION_SQL);
        await sleep(50);
        
        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [
                    TEST_FUNC1
                ],
                triggers: [
                    TEST_TRIGGER1
                ]
            },
            toCreate: {
                functions: [
                    ONLY_FUNCTION
                ],
                triggers: []
            }
        });
        assert.equal(counter, 1);
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([
                ONLY_FUNCTION
            ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers))
            .to.deep.equal([]);
    });
    
    it("change function on trigger", async() => {
        
        const filePath = ROOT_TMP_PATH + "/change-trigger.sql";
        fs.writeFileSync(filePath, ONLY_FUNCTION_SQL);
        

        let migration!: Migration;
        let counter = 0;

        const fsWatcher = await watch((_migration) => {
            migration = _migration;
            counter++;
        });
        
        
        fs.writeFileSync(filePath, TEST_FUNC1_SQL);
        await sleep(50);
        
        expect(migration).to.be.shallowDeepEqual({
            toDrop: {
                functions: [
                    ONLY_FUNCTION
                ],
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
        assert.equal(counter, 1);
        
        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([
                TEST_FUNC1
            ]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers))
            .to.be.shallowDeepEqual([
                TEST_TRIGGER1
            ]);
    });
});