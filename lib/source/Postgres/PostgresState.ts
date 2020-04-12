import { AbstractDDLState } from "../../DDLState";
import { TableDBO } from "./objects/TableDBO";
import { ViewDBO } from "./objects/ViewDBO";
import { TriggerDBO } from "./objects/TriggerDBO";
import { FunctionDBO } from "./objects/FunctionDBO";

export class PostgresState
extends AbstractDDLState<{
    table: TableDBO;
    view: ViewDBO;
    trigger: TriggerDBO;
    function: FunctionDBO;
}> {}