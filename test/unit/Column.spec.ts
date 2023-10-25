import { Column } from "../../lib/database/schema/Column";
import { TableID } from "../../lib/database/schema/TableID";
import assert from "assert";

describe("Column", () => {

    it("correct equal default values", () => {
        const column1 = new Column(
            new TableID("public", "test"),
            "my_column",
            "text",
            null
        );
        const column2 = new Column(
            new TableID("public", "test"),
            "my_column",
            "text",
            "null"
        );
        const column3 = new Column(
            new TableID("public", "test"),
            "my_column",
            "text",
            "''::text"
        );

        assert.ok( column1.same(column2), "column1 equal column2");
        assert.ok( column2.same(column1), "column2 equal column1");

        assert.ok( !column1.same(column3), "column1 !equal column3");
        assert.ok( !column2.same(column3), "column2 !equal column3");

        assert.ok( column3.same(column3), "column3 equal column3");
    });

    it("same float8 and double precision", () => {
        const column1 = new Column(
            new TableID("public", "test"),
            "my_column",
            "float8",
            null
        );
        const column2 = new Column(
            new TableID("public", "test"),
            "my_column",
            "double precision",
            "null"
        );

        assert.ok( column1.same(column2), "column1 equal column2");
        assert.ok( column2.same(column1), "column2 equal column1");
    });

})