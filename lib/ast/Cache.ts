import { Select } from "./Select";
import { TableReference } from "./TableReference";

export class Cache {
    readonly name: string;
    readonly for: TableReference;
    readonly select: Select;
    readonly withoutTriggers: string[];

    constructor(
        name: string,
        forTable: TableReference,
        select: Select,
        withoutTriggers: string[] = []
    ) {
        this.name = name;
        this.for = forTable;
        this.select = select;
        this.withoutTriggers = withoutTriggers;
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