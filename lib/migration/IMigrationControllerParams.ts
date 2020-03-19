import State from "../state/State";

export type TMigrationMode = "dev" | "prod";

export interface IMigrationControllerParams {
    fs: State; 
    db: State;
    mode: TMigrationMode;
}