import { GrapeQLCoach, Select } from "grapeql-lang";
import { CacheParser } from "../../../../lib/parser";
import { Table } from "../../../../lib/ast";
import { buildFromAndWhere } from "../../../../lib/cache/processor/buildFromAndWhere";
import assert from "assert";

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
        const triggerTable = new Table(schemaName, tableName);

        const {from, where} = buildFromAndWhere(cache, triggerTable);

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
