import { ColumnReference } from "../../../../ast";
import { TableReference } from "../../../../database/schema/TableReference";
import { IJoin } from "../../../processor/buildJoinVariables";

export interface ICombinedJoin {
    variables: string[];
    joinedColumns: string[];
    joinedTable: TableReference;
    byColumn: ColumnReference;
}

export function groupJoinsByTableAndFilter(joins: IJoin[]) {
    const combinedJoinByKey: {[key: string]: ICombinedJoin} = {};

    for (const join of joins) {
        const key = join.on.column.toString();

        const defaultCombinedJoin: ICombinedJoin = {
            variables: [],
            joinedColumns: [],
            joinedTable: join.table.ref,
            byColumn: join.on.column
        };
        const combinedJoin: ICombinedJoin = combinedJoinByKey[ key ] || defaultCombinedJoin;

        combinedJoin.variables.push(
            join.variable.name
        );
        combinedJoin.joinedColumns.push(
            join.table.column.name
        );
        combinedJoinByKey[ key ] = combinedJoin;
    }

    const combinedJoins = Object.values(combinedJoinByKey);
    return combinedJoins;
}
