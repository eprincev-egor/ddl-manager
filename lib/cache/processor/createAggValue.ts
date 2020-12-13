import { Expression } from "../../ast";
import { TableID } from "../../database/schema/TableID";
import { IJoinMeta } from "./findJoinsMeta";
import { replaceTriggerTableToRow } from "./replaceTriggerTableToRow";

export function createAggValue(
    triggerTable: TableID,
    joinsMeta: IJoinMeta[],
    aggArgs: Expression[],
    row: "new" | "old"
): Expression {
    return replaceTriggerTableToRow(
        aggArgs[0],
        triggerTable,
        joinsMeta,
        row
    );
}
