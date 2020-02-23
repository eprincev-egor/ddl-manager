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

    it("validate primaryKey, empty array", () => {
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
                    primaryKey: []
                });
            },
            err =>
                err.message === "primary key cannot be empty array"
        );
    });

    it("validate primaryKey, unknown column", () => {
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
                    primaryKey: ["id", "name"]
                });
            },
            err =>
                err.message === "unknown primary key columns: name"
        );
    });

    it("validate uniqueConstraint, empty array", () => {
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
                    uniqueConstraints: [
                        {
                            identify: "test",
                            name: "test",
                            unique: []
                        }
                    ]
                });
            },
            err =>
                err.message === "unique constraint 'test' cannot be empty array"
        );
    });

    it("validate uniqueConstraint, unknown column", () => {
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
                    uniqueConstraints: [
                        {
                            identify: "test",
                            name: "test",
                            unique: ["name"]
                        }
                    ]
                });
            },
            err =>
                err.message === "unique constraint 'test' contain unknown columns: name"
        );
    });

});