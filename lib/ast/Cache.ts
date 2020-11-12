import { Select } from "./Select";
import { TableReference } from "./TableReference";

export class Cache {
    readonly name: string;
    readonly for: TableReference;
    readonly select: Select;

    constructor(name: string, forTable: TableReference, select: Select) {
        this.name = name;
        this.for = forTable;
        this.select = select;
    }

    getSignature() {
        return `cache ${this.name} for ${this.for}`;
    }
}