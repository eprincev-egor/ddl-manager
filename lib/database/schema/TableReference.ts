import { TableID } from "./TableID";

export interface IReferenceFilter {
    schema?: string;
    aliasOrTableName: string;
}

export class TableReference {

    static identifier2filter(identifier: string): IReferenceFilter {
        if ( identifier.includes(".") ) {
            return {
                schema: identifier.split(".")[0],
                aliasOrTableName: identifier.split(".")[1]
            };
        }
        else {
            return {
                aliasOrTableName: identifier
            }
        }
    }

    readonly table: TableID;
    readonly alias?: string;

    constructor(table: TableID, alias?: string) {
        this.table = table;
        this.alias = alias;
    }

    getIdentifier() {
        return this.alias || this.table.toString();
    }

    matched(filter: IReferenceFilter) {
        if ( filter.schema ) {
            return (
                !this.alias &&
                this.table.schema === filter.schema &&
                this.table.name === filter.aliasOrTableName
            );
        }

        if ( this.alias ) {
            return this.alias === filter.aliasOrTableName;
        }

        return this.table.name === filter.aliasOrTableName;
    }

    equal(otherTable: TableReference | TableID) {
        if ( otherTable instanceof TableReference ) {
            return (
                otherTable.alias === this.alias &&
                otherTable.table.equal(this.table)
            );
        }
        return this.table.equal(otherTable);
    }

    clone() {
        return new TableReference(new TableID(
            this.table.schema,
            this.table.name
        ), this.alias);
    }

    toString() {
        const tableSQL = this.table.toStringWithoutPublic();

        if ( this.alias ) {
            return `${tableSQL} as ${this.alias}`;
        }
        
        return tableSQL;
    }
}