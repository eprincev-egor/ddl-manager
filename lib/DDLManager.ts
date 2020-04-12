
// error handling

import { IDBOSource, IDBODestination, IDBOWatcher } from "./common";
import { parallel } from "./utils/parallel";
import { Migrator } from "./migrator/Migrator";

export interface IDDLManagerParams {
    source: IDBOSource & IDBODestination & IDBOWatcher;
    destination: IDBOSource & IDBODestination;
}

export class DDLManager {
    private source: IDBOSource & IDBODestination & IDBOWatcher;
    private destination: IDBOSource & IDBODestination;
    private migrator: Migrator;

    constructor(params: IDDLManagerParams) {
        this.source = params.source;
        this.destination = params.destination;

        this.migrator = new Migrator({
            source: this.source, 
            destination: this.destination
        });
    }

    async build(): Promise<void> {
        await build(this.source, this.destination);
    }

    async dump(): Promise<void> {
        await build(this.destination, this.source);
    }

    async watch(): Promise<void> {
        await this.build();

        // debounce + queue
        let timer;
        this.source.watch(() => {
            clearTimeout(timer);

            timer = setTimeout(() => {
                this.onChange();
            }, 100);
        });
    }

    private async onChange() {
        await this.build();
    }
}

async function build(source: IDBOSource, destination: IDBOSource & IDBODestination) {
    await parallel([
        source.load(),
        destination.load()
    ]);
    const changes = destination.compare(source);
    destination.applyChanges(changes);
}
