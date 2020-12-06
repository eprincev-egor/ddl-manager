import { File } from "./File";

export class FSEvent {
    readonly created: File[];
    readonly removed: File[];

    constructor(changes: {created?: File[], removed?: File[]} = {}) {
        this.created = changes.created || [];
        this.removed = changes.removed || [];
    }

    create(file: File): FSEvent {
        const newEvent = new FSEvent({
            created: this.created.concat([file]),
            removed: this.removed
        });
        return newEvent;
    }

    remove(file: File): FSEvent {
        const newEvent = new FSEvent({
            created: this.created,
            removed: this.removed.concat([file])
        });
        return newEvent;
    }
}