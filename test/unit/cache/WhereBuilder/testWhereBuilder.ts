import { GrapeQLCoach, Select } from "grapeql-lang";
import { CacheParser } from "../../../../lib/parser";
import assert from "assert";
import { CacheContext } from "../../../../lib/cache/trigger-builder/CacheContext";
import { TableID } from "../../../../lib/database/schema/TableID";
import { Database } from "../../../../lib/database/schema/Database";
import { buildUniversalWhere } from "../../../../lib/cache/processor/buildUniversalWhere";
import { buildFrom } from "../../../../lib/cache/processor/buildFrom";

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
            new Database()
        );
        const from = buildFrom(context);
        const where = buildUniversalWhere(context);

        let actualSelect = `select from ${from.join(", ")}`;
        if ( where.toString().trim() ) {
            actualSelect += ` where\n${where}`;
        }
        const actualSelectSyntax = new GrapeQLCoach(actualSelect).parse(Select);

        const expectedFromAndWhere = test.where[ changedSchemaTable ];
        const expectedSelect = "select " + expectedFromAndWhere;
        const expectedSelectSyntax = new GrapeQLCoach(expectedSelect).parse(Select);

        assert.strictEqual(
            actualSelectSyntax.toString(),
            expectedSelectSyntax.toString(),
            `where clause by ${ changedSchemaTable }`
        );
    }
}
