import TableModel from "../lib/objects/TableModel";
import assert from "assert";

describe("TableModel", () => {

    it("validate columns, columns should be only actual or only deprecated", () => {
        assert.throws(
            () => {
                const model = new TableModel({
                    identify: "test",
                    name: "test",
                    columns: [
                        {
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }
                    ],
                    deprecatedColumns: ["id"]
                });
            },
            err =>
                err.message === "columns should be only actual or only deprecated: id"
        );
    });

    it("validate rows, unknown row key", () => {
        assert.throws(
            () => {
                const model = new TableModel({
                    identify: "test",
                    name: "test",
                    columns: [
                        {
                            identify: "id",
                            key: "id",
                            type: "integer"
                        }
                    ],
                    rows: [
                        {
                            id: 1,
                            name: "some"
                        }
                    ]
                });
            },
            err =>
                err.message === "unknown row columns: name"
        );
    });

});