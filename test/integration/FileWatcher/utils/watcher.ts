import { FileWatcher } from "../../../../lib/fs/FileWatcher";
import { MainComparator } from "../../../../lib/Comparator/MainComparator";
import { Migration } from "../../../../lib/Migrator/Migration";
import { prepare } from "../../utils/prepare";
import { Database } from "../../../../lib/database/schema/Database";
import { FakeDatabaseDriver } from "../../../unit/FakeDatabaseDriver";
import {
    TEST_TABLE_1
} from "../../fixture/triggers";

export function watcher(ROOT_TMP_PATH: string) {
    
    prepare(ROOT_TMP_PATH);

    const WATCHERS_TO_STOP: any[] = [];
    afterEach(() => {

        WATCHERS_TO_STOP.forEach(fsWatcher => 
            fsWatcher.stopWatch()
        );
    });

    async function watch(
        onChange?: (migration: Migration) => void,
        testDatabase?: Database
    ) {
        const postgres = new FakeDatabaseDriver();
        const database = testDatabase || new Database();

        if ( !testDatabase ) {
            database.setTable(TEST_TABLE_1);
        }

        const fsWatcher = await FileWatcher.watch([ROOT_TMP_PATH]);
        WATCHERS_TO_STOP.push(fsWatcher);

        const migration = await MainComparator.compare(
            postgres,
            database,
            fsWatcher.state
        );
        database.applyMigration(migration);

        if ( onChange ) {
            fsWatcher.on("change", async() => {
                const migration = await MainComparator.compare(
                    postgres,
                    database,
                    fsWatcher.state
                );
                onChange(migration);

                database.applyMigration(migration);
            });
        }
        
        return fsWatcher;
    }

    return {
        watch
    };
}