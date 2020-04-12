import { IDBO } from "./objects";
import { DDLState } from "../state/DDLState";

export interface IDBOSource {
    state: DDLState;
    load(): Promise<void>;
}

export interface IDBODestination {
    state: DDLState;
    create(dbo: IDBO): Promise<void>;
    drop(dbo: IDBO): Promise<void>;
}

export interface IDBOWatcher {
    watch(handlers: () => void);
}
