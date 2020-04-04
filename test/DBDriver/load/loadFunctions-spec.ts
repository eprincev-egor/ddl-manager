import path from "path";
import {TestFixtures} from "./TestFixtures";
import { FunctionModel } from "../../../lib/objects/FunctionModel";

describe("PgDBDriver: load functions", () => {

    const test = new TestFixtures({
        fixturesPath: path.join(__dirname, "func-fixtures"),
        load: async(driver) => 
            driver.loadFunctions(),
        prepareDDL(ddl: string) {
            return fixLineBreaks(ddl);
        },
        prepareDBO(dbo: FunctionModel["TJson"]) {
            fixLineBreaksInFunc(dbo);
        }
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

    function fixLineBreaks(str) {
        return str.replace(/[\r\n]+/g, "\n");
    }

    function fixLineBreaksInFunc(func: FunctionModel["TJson"]) {
        if ( !func.parsed ) {
            return;
        }
        
        const body = func.parsed.body;
        if ( !body || !body.content ) {
            return;
        }

        body.content = fixLineBreaks(body.content);
    }
});