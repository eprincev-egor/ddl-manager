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

        assert.ok( column1.suit(column2), "column1 equal column2");
        assert.ok( column2.suit(column1), "column2 equal column1");

        assert.ok( !column1.suit(column3), "column1 !equal column3");
        assert.ok( !column2.suit(column3), "column2 !equal column3");

        assert.ok( column3.suit(column3), "column3 equal column3");
    });

})