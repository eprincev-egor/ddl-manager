import { IDBO } from "./objects";
import { AbstractDDLState } from "../DDLState";

export interface IDBOSource {
    state: AbstractDDLState<any>;
    load(): Promise<void>;
}

export interface IDBODestination {
    state: AbstractDDLState<any>;
    create(dbo: IDBO): Promise<void>;
    drop(dbo: IDBO): Promise<void>;
}

export interface IDBOWatcher {
    watch(handlers: () => void);
}
