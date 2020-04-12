
export class Queue<TChanges> {
    private stack: TChanges[];
    private errorHandler: (err: Error) => void;
    private changesHandler: (changes: TChanges) => Promise<void>;

    constructor() {
        this.stack = [];
    }

    push(changes: TChanges) {
        this.stack.push(changes);
        this.execute();
    }

    private async execute() {
        const changes = this.stack.shift();
        
        try {
            await this.changesHandler(changes);
        } catch(err) {
            this.errorHandler(err);
        }
    }

    async onPush(
        changesHandler: (changes: TChanges) => Promise<void>
    ) {
        this.changesHandler = changesHandler;
    }

    onError(errorHandler: (err: Error) => void) {
        this.errorHandler = errorHandler;
    }
}