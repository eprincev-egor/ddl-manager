import { Expression } from "./expression";

export type IndexTarget = Expression | string;

export class CacheIndex {
    readonly index: string;
    readonly on: IndexTarget[];

    constructor(index: string, on: IndexTarget[]) {
        this.index = index;
        this.on = on;
    }

    toString() {
        return `index ${this.index} on (${this.on.join(", ")})`;
    }
}