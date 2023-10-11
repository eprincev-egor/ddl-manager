import assert from "assert";
import {
    findDependencies, IDependencies
} from "../../../../lib/cache/processor/findDependencies";
import { FileParser } from "../../../../lib/parser";

interface ITest {
    cache: string;
    runOnlyMe?: boolean;
    dependencies?: IDependencies;
    error?: RegExp;
}

let runOnlyTest: ITest | null;
export function testDependencies(test: ITest) {

    if ( test.runOnlyMe ) {
        runOnlyTest = test;
    }

    it(test.cache, () => {
        if ( runOnlyTest ) {
            if ( !test.runOnlyMe ) {
                return;
            }
        }

        if ( test.error ) {
            const regExp = test.error;
            assert.throws(
                () => {
                    const file = FileParser.parse(test.cache);
                    const cache = file?.cache[0]!;
                    findDependencies(cache);
                },
                (err: Error) =>
                    regExp.test(err.message)
            );
        }
        else {
            const file = FileParser.parse(test.cache);
            const cache = file?.cache[0]!;
            const json = findDependencies(cache);
            assert.deepStrictEqual(json, test.dependencies);
        }
    });
}