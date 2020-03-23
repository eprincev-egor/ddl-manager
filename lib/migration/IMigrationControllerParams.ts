import {DDLState} from "../state/DDLState";
import {FSDDLState} from "../state/FSDDLState";

export type TMigrationMode = "dev" | "prod";

export interface IMigrationControllerParams {
    fs: FSDDLState;
    db: DDLState;
    mode: TMigrationMode;
}