import path from "path";
import {TestFixtures} from "./TestFixtures";

describe("PgDBDriver: load triggers", () => {

    const test = new TestFixtures({
        fixturesPath: path.join(__dirname, "trigger-fixtures"),
        load: (driver) => driver.loadTriggers()
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