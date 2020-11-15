import { Join } from "./Join";
import { TableReference } from "./TableReference";

export class From {
    readonly table: TableReference;
    readonly joins: Join[];
    constructor(table: TableReference, joins: Join[] = []) {
        this.table = table;
        this.joins = joins;
    }

    addJoin(join: Join) {
        const newJoins = [
            ...this.joins.map(oldJoin=> oldJoin.clone()),
            join
        ];
        const clone = new From(
            this.table,
            newJoins
        );
        return clone;
    }

    clone() {
        return new From(
            this.table.clone(),
            this.joins.map(join => join.clone())
        );
    }

    toString() {
        let sql = `${this.table}`;

        if ( this.joins.length ) {
            sql += " ";
            sql += this.joins.join("\n");
        }

        return sql;
    }
}