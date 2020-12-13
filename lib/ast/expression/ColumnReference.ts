import { Spaces } from "../Spaces";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { UnknownExpressionElement } from "./UnknownExpressionElement";

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

    replaceColumn(replaceColumn: string, toSql: string) {
        if ( this.toString() === replaceColumn ) {
            return UnknownExpressionElement.fromSql(toSql);
        }
        return this.clone();
    }

    getColumnReferences() {
        return [this];
    }

    clone() {
        return new ColumnReference(this.tableReference.clone(), this.name);
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
}