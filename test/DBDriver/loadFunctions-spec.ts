import path from "path";
import {TestFixtures} from "./TestFixtures";

describe("PgDBDriver: load functions", () => {

    const test = new TestFixtures(
        path.join(__dirname, "func-fixtures"),
        (driver) => driver.loadFunctions()
    );

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