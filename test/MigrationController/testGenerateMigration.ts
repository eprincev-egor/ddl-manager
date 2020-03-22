import DDLState, {IMigrationOptions} from "../../lib/state/DDLState";
import MigrationModel from "../../lib/migration/MigrationModel";
import assert from "assert";

type InputState = DDLState["TInputData"];
interface IGenerateMigrationTest {
    options?: IMigrationOptions;
    fs: InputState;
    db: InputState;
    migration: MigrationModel["TJson"];
};

export default function testGenerateMigration(test: IGenerateMigrationTest) {
    
    const fsState = new DDLState(test.fs);
    const dbState = new DDLState(test.db);

    const migration = fsState.generateMigration(dbState, test.options);
    assert.deepStrictEqual(migration.toJSON(), test.migration);
}