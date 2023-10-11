export class CacheIndex {
    constructor(
        readonly index: string,
        readonly on: string[]
    ) {}

    toString() {
        return `index ${this.index} on (${this.on.join(", ")})`;
    }
}