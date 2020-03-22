import DDLState from "../../lib/state/DDLState";
import FSDDLState, {IMigrationOptions} from "../../lib/state/FSDDLState";
import MigrationModel from "../../lib/migration/MigrationModel";
import MainMigrationController from "../../lib/migration/MainMigrationController";
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

    const controller = new MainMigrationController({
        ...test.options,
        fs: fsState,
        db: dbState
    });

    const migration = controller.generateMigration();
    assert.deepStrictEqual(migration.toJSON(), test.migration);
}