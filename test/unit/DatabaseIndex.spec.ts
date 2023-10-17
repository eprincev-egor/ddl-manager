import { Index } from "../../lib/database/schema/Index";
import { TableID } from "../../lib/database/schema/TableID";
import assert from "assert";

describe("DatabaseIndex", () => {

    it("equal two similar indexes with long names", () => {
        const index1 = new Index({
            name: "long_name".repeat(100),
            table: TableID.fromString("public.table"),
            index: "btree",
            columns: ["id"]
        });

        const index2 = new Index({
            name: "long_name".repeat(80),
            table: TableID.fromString("public.table"),
            index: "btree",
            columns: ["id"]
        });
        
        assert.ok( index1.equal(index2), "index1 == index2" );
        assert.ok( index2.equal(index1), "index2 == index1" );
    });

})