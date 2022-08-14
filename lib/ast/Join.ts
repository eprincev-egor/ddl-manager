import { Expression } from "./expression/Expression";
import { TableReference } from "../database/schema/TableReference";
import { TableID } from "../database/schema/TableID";

export class Join {
    readonly type: string;
    readonly table: TableReference;
    readonly on: Expression;
    constructor(type: string, table: TableReference, on: Expression) {
        this.type = type;
        this.table = table;
        this.on = on;
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return new Join(
            this.type,
            this.table.clone(),
            this.on.replaceTable(replaceTable, toTable)
        );
    }

    equal(join: Join) {
        return (
            this.type === join.type &&
            this.table.equal(join.table) &&
            this.on.equal(join.on)
        );
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