import { ColumnReference as ColumnLink, FromTable, Select } from "psql-lang";
import { ColumnReference } from "../ast";
import { CacheSyntax } from "./CacheSyntax";
import { strict } from "assert";
import { parseFromTable } from "./utils";

export class ColumnReferenceParser {

    parse(columnLink: ColumnLink) {
        const columnName = columnLink.last()!.toValue();
        const tableReference = this.findTableReference(columnLink);
        return new ColumnReference(tableReference, columnName);
    }

    private findTableReference(columnLink: ColumnLink) {
        const isStar = (
            columnLink.row.allColumns &&
            columnLink.row.column.length === 0
        );
        if ( isStar ) {
            return this.findFirstFrom(columnLink);
        }

        const fromItem = columnLink.findDeclaration() as FromTable | undefined;
        if ( fromItem ) {
            return parseFromTable(
                fromItem.row.table,
                fromItem.row.as
            );
        }

        if ( columnLink.row.column.length === 1 ) {
            return this.findFirstFrom(columnLink);
        }

        const cache = columnLink.findParentInstance(CacheSyntax);
        strict.ok(cache, `source for column ${columnLink} not found`);

        return parseFromTable(
            cache.row.for,
            cache.row.as
        );
    }

    private findFirstFrom(columnLink: ColumnLink) {
        const select = columnLink.findParentInstance(Select)!;
        const fromItem = select.row.from[0] as FromTable;
        return parseFromTable(
            fromItem.row.table,
            fromItem.row.as
        );
    }
}