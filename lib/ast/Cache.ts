import { Select } from "./Select";
import { TableReference } from "../database/schema/TableReference";
import { CacheIndex } from "./CacheIndex";

export class Cache {
    readonly name: string;
    readonly for: TableReference;
    readonly select: Select;
    readonly withoutTriggers: string[];
    readonly indexes: CacheIndex[];

    constructor(
        name: string,
        forTable: TableReference,
        select: Select,
        withoutTriggers: string[] = [],
        indexes: CacheIndex[] = []
    ) {
        this.name = name;
        this.for = forTable;
        this.select = select;
        this.withoutTriggers = withoutTriggers;
        this.indexes = indexes;
    }

    equal(otherCache: Cache) {
        return (
            this.name === otherCache.name &&
            this.for.equal(otherCache.for) &&
            this.select.toString() === otherCache.select.toString() &&
            this.withoutTriggers.join(",") === otherCache.withoutTriggers.join(",")
        );
    }

    getSignature() {
        return `cache ${this.name} for ${this.for}`;
    }

    toString() {
        return `
cache ${this.name} for ${this.for} (
    ${this.select}
)
${ this.withoutTriggers.map(onTable => 
    `without triggers on ${onTable}`
).join(" ").trim() }
        `;
    }
}