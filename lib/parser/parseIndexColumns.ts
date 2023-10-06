import { ColumnReference, OrderByItem, Sql } from "psql-lang";

export function parseIndexColumns(columnsStr: string) {
    const {cursor} = Sql.code(columnsStr);
    cursor.skipSpaces();

    const indexItems = cursor.parseChainOf(OrderByItem, ",");
    return indexItems.map(indexItemNode => {
        const indexItem = indexItemNode.row;
        const isJustColumn = (
            indexItem.expression instanceof ColumnReference &&
            indexItem.vector === "asc" &&
            !indexItem.nulls
        );
        if ( isJustColumn ) {
            return indexItem.expression.toString();
        }

        return indexItemNode.toString();
    });
}