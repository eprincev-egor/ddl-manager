"use strict";

const assert = require("assert");
const DDLCoach = require("../../lib/parser/DDLCoach");
const weakDeepEqual = require("./weakDeepEqual");

function testSyntax(className, test) {
    it(test.str, () => {

        let str = test.str;
        let parseFuncName = "parse" + className;

        if ( test.error ) {
            try {
                let coach = new DDLCoach(str);
                coach[ parseFuncName ]();
                assert.ok(false, "expected error");
            } catch(err) {
                assert.ok(err.message != "expected error", "expected error: " + str);
            }
        }

        else if ( test.result ) {
            let coach = new DDLCoach(str);
            let result;
            if ( test.options ) {
                result = coach[ parseFuncName ](test.options);
            } else {
                result = coach[ parseFuncName ]();
            }

            let isEqual = !!weakDeepEqual(test.result, result);
            if ( !isEqual ) {
                console.log("break here");
                console.log(result);
            }

            assert.ok(isEqual);


            // auto test clone and toString
            let clone = result.clone();
            let cloneString = clone.toString();
            let cloneCoach = new DDLCoach( cloneString );
            
            let cloneResult;
            if ( test.options ) {
                cloneResult = cloneCoach[ parseFuncName ](test.options);
            } else {
                cloneResult = cloneCoach[ parseFuncName ]();
            }

            isEqual = !!weakDeepEqual(test.result, cloneResult);
            if ( !isEqual ) {
                console.log("clone, break here");
                console.log(cloneResult);
            }
            assert.ok(isEqual);
        }
        else {
            throw new Error("invalid test");
        }

    });
}

module.exports = testSyntax;
