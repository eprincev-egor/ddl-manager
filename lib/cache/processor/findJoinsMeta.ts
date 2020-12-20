import { flatMap } from "lodash";
import { Select } from "../../ast";

const SIMPLE_JOIN_TYPES = [
    "left join",
    "inner join"
];

export interface IJoinMeta {
    joinAlias?: string;
    joinedColumns: string[];
    joinedTable: string;
    joinByColumn: string;
}

export function findJoinsMeta(select: Select) {
    const outputJoins: IJoinMeta[] = [];
    const allColumnsRefs = flatMap(select.columns, selectColumn => 
        selectColumn.expression.getColumnReferences()
    );
    const simpleJoins = flatMap(select.from, fromItem => fromItem.joins)
        .filter(join => 
            SIMPLE_JOIN_TYPES.includes(join.type)
        );

    for (const join of simpleJoins) {

        const joinByColumn = join.on.getColumnReferences().find(joinConditionColumn =>
            !joinConditionColumn.tableReference.equal(join.table)
        );
        if ( !joinByColumn ) {
            continue;
        }

        const joinMeta: IJoinMeta = {
            joinAlias: join.table.alias,
            joinedTable: join.table.table.toStringWithoutPublic(),
            joinedColumns: [],
            joinByColumn: joinByColumn.toString()
        };

        const columnRefsToJoin = allColumnsRefs.filter(columnRef =>
            columnRef.tableReference.equal(join.table)
        );
        for (const columnRef of columnRefsToJoin) {
            if ( joinMeta.joinedColumns.includes(columnRef.name) ) {
                continue;
            }

            joinMeta.joinedColumns.push(
                columnRef.name
            );
        }

        if ( joinMeta.joinedColumns.length ) {
            outputJoins.push( joinMeta );
        }
    }

    return outputJoins;
}
