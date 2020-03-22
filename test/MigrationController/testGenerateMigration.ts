import DDLState from "../../lib/state/DDLState";
import FSDDLState, {IMigrationOptions} from "../../lib/state/FSDDLState";
import MigrationModel from "../../lib/migration/MigrationModel";
import assert from "assert";

interface IGenerateMigrationTest {
    options?: IMigrationOptions;
    fs: FSDDLState["TInputData"];
    db: DDLState["TInputData"];
    migration: MigrationModel["TJson"];
};

export default function testGenerateMigration(test: IGenerateMigrationTest) {
    
    const fsState = new FSDDLState(test.fs);
    const dbState = new DDLState(test.db);

    const migration = fsState.generateMigration(dbState, test.options);
    assert.deepStrictEqual(migration.toJSON(), test.migration);
}