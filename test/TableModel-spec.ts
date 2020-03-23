import {TableModel} from "../lib/objects/TableModel";
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

    it("validate foreignKeyConstraint, empty array of columns", () => {
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
                    foreignKeysConstraints: [
                        {
                            identify: "country_fk",
                            name: "country_fk",
                            columns: [],
                            referenceTableIdentify: "country",
                            referenceColumns: ["id"]
                        }
                    ]
                });
            },
            err =>
                err.message === "columns inside foreign key constraint 'country_fk' cannot be empty array"
        );
    });

    it("validate foreignKeyConstraint, empty array of referenceColumns", () => {
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
                        },
                        {
                            identify: "id_country",
                            key: "id_country",
                            type: "integer"
                        }
                    ],
                    foreignKeysConstraints: [
                        {
                            identify: "country_fk",
                            name: "country_fk",
                            columns: ["id_country"],
                            referenceTableIdentify: "country",
                            referenceColumns: []
                        }
                    ]
                });
            },
            err =>
                err.message === "referenceColumns inside foreign key constraint 'country_fk' cannot be empty array"
        );
    });

    
    it("validate foreign key constraint, unknown columns", () => {
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
                    foreignKeysConstraints: [
                        {
                            identify: "country_fk",
                            name: "country_fk",
                            columns: ["id_country"],
                            referenceTableIdentify: "country",
                            referenceColumns: ["id"]
                        }
                    ]
                });
            },
            err =>
                err.message === "foreign key constraint 'country_fk' contain unknown columns: id_country"
        );
    });

});