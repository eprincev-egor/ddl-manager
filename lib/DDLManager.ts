
// error handling

interface IChanges {
    removed: any[];
    created: any[];
    changed: any[];
}

export interface IDDLSource {
    load(): Promise<void>;
    applyChanges(changes): Promise<void>;
    compare(anotherSource: IDDLSource): IChanges;
    watch(handler: (changes) => void): void;
}

export interface IDDLManagerParams {
    source: IDDLSource;
    destination: IDDLSource;
}

export class DDLManager {
    private source: IDDLSource;
    private destination: IDDLSource;

    constructor(params: IDDLManagerParams) {
        this.source = params.source;
        this.destination = params.destination;
    }

    async build(): Promise<void> {
        await build(this.source, this.destination);
    }

    async dump(): Promise<void> {
        await build(this.destination, this.source);
    }

    async watch(): Promise<void> {
        await this.build();

        // const queue = new Queue();

        this.source.watch(async(changes) => {
            // queue.push(changes);
        });

        // queue.execute(async (changes) => {
        //     await this.destination.applyChanges(changes);
        // });

        // queue.catch((err) => {
        //     // this.source
        // });
    }
}

async function build(source: IDDLSource, destination: IDDLSource) {
    await parallel([
        source.load(),
        destination.load()
    ]);
    const changes = destination.compare(source);
    destination.applyChanges(changes);
}

async function parallel(promises: Promise<any>[]) {
    await Promise.all(promises);
}