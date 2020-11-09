import { Expression } from "./expression/Expression";
import { TableReference } from "./TableReference";

export class Join {
    readonly type: string;
    readonly table: TableReference;
    readonly on: Expression;
    constructor(type: string, table: TableReference, on: Expression) {
        this.type = type;
        this.table = table;
        this.on = on;
    }

    clone() {
        return new Join(
            this.type,
            this.table.clone(),
            this.on.clone()
        );
    }

    toString() {
        return `${this.type} ${this.table} on\n${this.on}`;
    }
}