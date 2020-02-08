import State from "../lib/State";
import Migration from "../lib/migration/Migration";
import assert from "assert";

type InputState = State["TInputData"];
interface IGenerateMigrationTest {
    fs: InputState;
    db: InputState;
    migration: Migration["TJson"];
};

export default function testGenerateMigration(test: IGenerateMigrationTest) {
    
    const fsState = new State(test.fs);
    const dbState = new State(test.db);

    const migration = fsState.generateMigration(dbState);
    assert.deepStrictEqual(migration.toJSON(), test.migration);
}