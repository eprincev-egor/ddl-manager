import { Expression } from "./expression/Expression";
import { TableReference } from "../database/schema/TableReference";
import { TableID } from "../database/schema/TableID";
import { HardCode } from "./HardCode";
import { strict } from "assert";
import { Spaces } from "./Spaces";

export class Join {
    readonly type: string;
    readonly table: TableReference | HardCode;
    readonly on: Expression;
    constructor(
        type: string,
        table: TableReference | HardCode,
        on: Expression
    ) {
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
            this.table.toString() === join.table.toString() &&
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

    template(spaces: Spaces) {
        return [
            spaces + `${this.type} ${this.table} on`,
            spaces.plusOneLevel() + this.on.toString()
        ];
    }

    toString() {
        return `${this.type} ${this.table} on\n${this.on}`;
    }

    getTableId() {
        return this.getTable().table;
    }

    getTable(): TableReference {
        strict.ok(this.table instanceof TableReference, "expected join table");
        return this.table;
    }
}