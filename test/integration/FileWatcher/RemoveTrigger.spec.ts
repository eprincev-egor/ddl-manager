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

describe("integration/FileWatcher watch remove triggers", () => {
    
    const ROOT_TMP_PATH = __dirname + "/tmp";
    const {watch} = watcher(ROOT_TMP_PATH);
    
    it("remove trigger", async() => {
        
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
                triggers: [
                    TEST_TRIGGER1
                ]
            },
            toCreate: {
                functions: [],
                triggers: []
            }
        });

        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers))
            .to.be.shallowDeepEqual([]);
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
                triggers: [
                    TEST_TRIGGER1
                ]
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
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers))
            .to.be.shallowDeepEqual([
                TEST_TRIGGER2
            ]);


        fs.unlinkSync(filePath2);
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
                functions: [],
                triggers: []
            }
        });


        expect(flatMap(fsWatcher.state.files, file => file.content.functions))
            .to.be.shallowDeepEqual([]);
        expect(flatMap(fsWatcher.state.files, file => file.content.triggers))
            .to.be.shallowDeepEqual([]);
    });
});