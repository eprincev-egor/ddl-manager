import { Spaces } from "../Spaces";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { IExpressionElement } from "./interface";
import { Cache } from "../Cache";

export class ColumnReference extends AbstractExpressionElement {
    readonly tableReference: TableReference;
    readonly name: string;

    constructor(tableReference: TableReference, name: string) {
        super();
        this.tableReference = tableReference;
        this.name = name;
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        if ( this.tableReference.equal(replaceTable) ) {
            return new ColumnReference(toTable, this.name);
        }
        return this.clone();
    }

    replaceColumn(replaceColumn: ColumnReference, toSql: IExpressionElement) {
        if ( this.equal(replaceColumn) ) {
            return toSql;
        }
        return this.clone();
    }

    equal(otherColumnRef: ColumnReference) {
        return (
            this.name === otherColumnRef.name &&
            this.tableReference.equal(otherColumnRef.tableReference)
        );
    }

    getColumnReferences() {
        return [this];
    }

    clone() {
        return new ColumnReference(this.tableReference.clone(), this.name);
    }

    isRefTo(
        cache: Cache,
        triggerTable: TableID,
        excludeRef: TableReference | false = cache.for
    ) {
        return (
            this.tableReference.table.equal(triggerTable) && (
                !excludeRef ||
                !this.tableReference.equal(excludeRef)
            )
        );
    }

    template(spaces: Spaces) {
        if ( this.tableReference.alias ) {
            return [`${this.tableReference.alias}.${this.name}`];
        }

        if ( this.tableReference.table.toStringWithoutPublic() === "." ) {
            return [this.name];
        }
        return [`${this.tableReference.table.toStringWithoutPublic()}.${this.name}`];
    }

    toId() {
        return `${this.tableReference.table}.${this.name}`;
    }
}