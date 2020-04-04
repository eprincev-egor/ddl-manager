import {CheckConstraintModel} from "../lib/objects/CheckConstraintModel";
import assert from "assert";

describe("CheckConstraintModel", () => {

    it("validate name", () => {
        assert.throws(
            () => {
                const constraint = new CheckConstraintModel({
                    identify: "test",
                    name: "test_not_null"
                });
            },
            err =>
                err.message === "invalid name: 'test_not_null', name cannot contain '_not_null'"
        );
    });

});