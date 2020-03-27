import {DDLState} from "../../lib/state/DDLState";
import {FSDDLState, IMigrationOptions} from "../../lib/state/FSDDLState";
import {MigrationModel} from "../../lib/migration/MigrationModel";
import {MainMigrator} from "../../lib/migration/MainMigrator";
import assert from "assert";

interface IGenerateMigrationTest {
    options?: IMigrationOptions;
    fs: FSDDLState["TInputData"];
    db: DDLState["TInputData"];
    migration: MigrationModel["TJson"];
};

export function testGenerateMigration(test: IGenerateMigrationTest) {
    
    const fsState = new FSDDLState(test.fs);
    const dbState = new DDLState(test.db);

    const migrator = new MainMigrator({
        ...test.options,
        fs: fsState,
        db: dbState
    });

    const migration = migrator.migrate();
    assert.deepStrictEqual(migration.toJSON(), test.migration);
}