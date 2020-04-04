import path from "path";
import {TestFixtures} from "./TestFixtures";

describe("PgDBDriver: load tables", () => {

    const test = new TestFixtures({
        fixturesPath: path.join(__dirname, "table-fixtures"),
        load: (driver) => driver.loadTables()
    });

    before(async() => {
        await test.before();
    });

    beforeEach(async() => {
        await test.beforeEach();
    });

    afterEach(async() => {
        await test.afterEach();
    });

    after(async() => {
        await test.after();
    });

    test.testFixtures();

});