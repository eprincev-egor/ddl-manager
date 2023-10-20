import { Sql, Select } from "psql-lang";
import { CacheParser } from "../../../../lib/parser";
import assert from "assert";
import { CacheContext } from "../../../../lib/cache/trigger-builder/CacheContext";
import { TableID } from "../../../../lib/database/schema/TableID";
import { Database } from "../../../../lib/database/schema/Database";
import { buildUniversalWhere } from "../../../../lib/cache/processor/buildUniversalWhere";
import { buildFrom } from "../../../../lib/cache/processor/buildFrom";
import { FilesState } from "../../../../lib/fs/FilesState";
import { CacheColumnGraph } from "../../../../lib/Comparator/graph/CacheColumnGraph";

interface ITest {
    cache: string;
    where: {
        [table: string]: string;
    };
}

export function testWhereBuilder(test: ITest) {
    const cache = CacheParser.parse(test.cache);

    for (const changedSchemaTable in test.where) {

        const [schemaName, tableName] = changedSchemaTable.split(".");
        const triggerTable = new TableID(schemaName, tableName);

        const context = new CacheContext(
            cache,
            triggerTable,
            [],
            new Database(),
            new FilesState(),
            new CacheColumnGraph([])
        );
        const from = buildFrom(context);
        const where = buildUniversalWhere(context);

        let actualSelect = `select from ${from.join(", ")}`;
        if ( where.toString().trim() ) {
            actualSelect += ` where\n${where}`;
        }
        const actualSelectSyntax = Sql.code(actualSelect).parse(Select);

        const expectedFromAndWhere = test.where[ changedSchemaTable ];
        const expectedSelect = "select " + expectedFromAndWhere;
        const expectedSelectSyntax = Sql.code(expectedSelect).parse(Select);

        assert.strictEqual(
            actualSelectSyntax.toString(),
            expectedSelectSyntax.toString(),
            `where clause by ${ changedSchemaTable }`
        );
    }
}
