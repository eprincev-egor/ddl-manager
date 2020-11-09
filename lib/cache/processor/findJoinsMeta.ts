import { Select, ColumnReference } from "../../ast";

const SIMPLE_JOIN_TYPES = [
    "left join",
    "inner join"
];

export interface IJoinMeta {
    joinAlias?: string;
    joinedColumn: string;
    joinedTable: string;
    joinByColumn: string;
}

export function findJoinsMeta(select: Select) {
    const joins: IJoinMeta[] = [];

    for (const column of select.columns) {
        for (const columnReference of column.expression.getColumnReferences()) {

            const joinMeta = findSimpleJoinMetaForColumn({
                select,
                columnReference
            });
            if ( joinMeta ) {
                joins.push(joinMeta);
            }
        }
    }

    return joins;
}

function findSimpleJoinMetaForColumn(params: {
    select: Select;
    columnReference: ColumnReference;
}): IJoinMeta | undefined {
    const {select, columnReference} = params;

    const sourceJoin = findSourceJoin(
        select,
        columnReference
    );

    if ( !sourceJoin ) {
        return;
    }
    
    const isSimpleJoinType = SIMPLE_JOIN_TYPES.includes(sourceJoin.type);
    if ( !isSimpleJoinType ) {
        return;
    }

    const joinByColumn = sourceJoin.on.getColumnReferences().find(joinConditionColumn =>
        !joinConditionColumn.tableReference.equal(sourceJoin.table)
    );
    if ( !joinByColumn ) {
        return;
    }
    
    const meta: IJoinMeta = {
        joinAlias: sourceJoin.table.alias,
        joinedTable: columnReference.tableReference.table.toStringWithoutPublic(),
        joinedColumn: columnReference.name,
        joinByColumn: joinByColumn.toString()
    };
    return meta;
}

function findSourceJoin(select: Select, columnReference: ColumnReference) {
    for (const from of select.from) {
        for (const join of from.joins) {
            if ( join.table.equal(columnReference.tableReference) ) {
                return join;
            }
        }
    }
}
