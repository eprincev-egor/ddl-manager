import { FileReader } from "../../../../lib/fs/FileReader";
import { MainComparator } from "../../../../lib/Comparator/MainComparator";
import { Migration } from "../../../../lib/Migrator/Migration";
import { prepare } from "../../utils/prepare";
import { FilesState } from "../../../../lib/fs/FilesState";
import { Database } from "../../../../lib/database/schema/Database";

export function watcher(ROOT_TMP_PATH: string) {
    
    prepare(ROOT_TMP_PATH);

    const WATCHERS_TO_STOP: any[] = [];
    afterEach(() => {

        WATCHERS_TO_STOP.forEach(fsWatcher => 
            fsWatcher.stopWatch()
        );
    });

    async function watch(
        onChange?: (migration: Migration) => void
    ) {
        const fsWatcher = FileReader.read({
            folder: ROOT_TMP_PATH
        });
        WATCHERS_TO_STOP.push(fsWatcher);

        if ( onChange ) {
            fsWatcher.on("change", (fsEvent) => {
                const migration = MainComparator.fsEventToMigration(
                    new Database(),
                    new FilesState(),
                    fsEvent
                );
                onChange(migration);
            });
        }
        
        await fsWatcher.watch();

        return fsWatcher;
    }

    return {
        watch
    };
}