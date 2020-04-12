import { IDBOSource, IDBODestination, IDBOWatcher } from "../common";

export interface IMigratorParams {
    source: IDBOSource & IDBODestination & IDBOWatcher;
    destination: IDBOSource & IDBODestination;
}

export class Migrator {
    private source: IDBOSource & IDBODestination & IDBOWatcher;
    private destination: IDBOSource & IDBODestination;

    constructor(params: IMigratorParams) {
        this.source = params.source;
        this.destination = params.destination;
    }

    async migrate() {
        
    }
}