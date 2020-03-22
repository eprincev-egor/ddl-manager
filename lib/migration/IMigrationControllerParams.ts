import DDLState from "../state/DDLState";

export type TMigrationMode = "dev" | "prod";

export interface IMigrationControllerParams {
    fs: DDLState; 
    db: DDLState;
    mode: TMigrationMode;
}