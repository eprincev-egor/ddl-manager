import { Join } from "./Join";
import { TableReference } from "../database/schema/TableReference";
import { TableID } from "../database/schema/TableID";
import { HardCode } from "./HardCode";
import { Select } from "./Select";
import { FuncCall } from "./expression";
import { Spaces } from "./Spaces";

export interface FromParams {
    source: FromSource;
    as?: string;
    joins?: Join[];
}

export type FromSource = (
    TableReference | 
    HardCode | 
    Select |
    FuncCall
);

export class From {
    readonly source: FromSource;
    readonly as?: string;
    readonly joins: Join[];
    constructor(params: FromParams) {
        this.source = params.source;
        this.as = params.as;
        this.joins = params.joins || [];
    }

    addJoin(join: Join) {
        const newJoins = [
            ...this.joins.map(oldJoin=> oldJoin.clone()),
            join
        ];
        return this.clone({
            joins: newJoins
        });
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        return this.clone({
            joins: this.joins.map(join => 
                join.replaceTable(replaceTable, toTable)
            )
        });
    }

    equal(from: From) {
        return (
            this.source.toString() === from.source.toString() &&

            this.joins.length === from.joins.length &&
            this.joins.every((join, i) => 
                join.equal(from.joins[i])
            )
        );
    }

    clone(params: Partial<FromParams> = {}) {
        return new From({
            source: this.source.clone(),
            as: this.as,
            joins: this.joins.map(join => join.clone()),
            ...params
        });
    }

    template(spaces: Spaces) {
        const output: string[] = [];

        if ( this.source instanceof TableReference ) {
            output.push(this.source.toString());
        }
        else if ( this.source instanceof Select ) {
            output.push(`(`);
            output.push(
                ...this.source.template(spaces.plusOneLevel())
            );
            output.push(`${spaces}) as ${this.as}`);
        }
        else {
            output.push(this.source.toString() + ` as ${this.as}`);
        }

        for (const join of this.joins) {
            output.push("");
            output.push(...join.template(spaces));
        }

        return output;
    }

    toString() {
        let sql = this.source instanceof TableReference ? 
            this.source : `(${this.source}) as ${this.as}`;

        if ( this.joins.length ) {
            sql += " ";
            sql += this.joins.join("\n");
        }

        return sql;
    }
}