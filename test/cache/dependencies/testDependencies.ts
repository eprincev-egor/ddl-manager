import assert from "assert";
import {
    findDependencies, IDependencies
} from "../../../lib/cache/processor/findDependencies";
import { CacheParser } from "../../../lib/parser/CacheParser";

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
                    const cache = CacheParser.parse(test.cache);
                    findDependencies(cache);
                },
                (err) =>
                    regExp.test(err.message)
            );
        }
        else {
            const cache = CacheParser.parse(test.cache);
            const json = findDependencies(cache);
            assert.deepStrictEqual(json, test.dependencies);
        }
    });
}