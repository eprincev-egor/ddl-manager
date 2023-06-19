import { flatMap } from "lodash";
import { ColumnReference, Select } from "../../ast";
import { TableReference } from "../../database/schema/TableReference";

export interface IJoinMeta {
    joinedColumns: ColumnReference[];
    joinedTable: TableReference;
    joinByColumn: ColumnReference;
}

export function findJoinsMeta(select: Select) {
    const outputJoins: IJoinMeta[] = [];
    // const selectColumnsRefs = flatMap(select.columns, selectColumn => 
    //     selectColumn.expression.getColumnReferences()
    // );
    const allColumnsRefs = select.getAllColumnReferences();

    const simpleJoins = flatMap(select.from, fromItem => fromItem.joins)
        .filter(join => 
            join.type === "left join"
        );

    for (const join of simpleJoins) {

        // TODO: join by two columns?
        // example: documents.table_name && documents.table_id
        const joinByColumn = join.on.getColumnReferences().find(joinConditionColumn =>
            !joinConditionColumn.tableReference.equal(join.getTable())
        );
        if ( !joinByColumn ) {
            continue;
        }

        const joinMeta: IJoinMeta = {
            joinedTable: join.getTable(),
            joinedColumns: [],
            joinByColumn
        };

        // TODO: need More test
        const columnRefsToJoin = allColumnsRefs.filter(columnRef =>
            columnRef.name !== "id" &&
            columnRef.tableReference.equal(join.getTable())
        );
        for (const columnRef of columnRefsToJoin) {
            const alreadyExists = joinMeta.joinedColumns.some(existentColumn => 
                existentColumn.name === columnRef.name
            );
            if ( alreadyExists ) {
                continue;
            }

            joinMeta.joinedColumns.push(
                columnRef
            );
        }

        if ( joinMeta.joinedColumns.length ) {
            outputJoins.push( joinMeta );
        }
    }

    return outputJoins;
}
