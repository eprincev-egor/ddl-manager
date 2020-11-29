
export class TableID {
    readonly schema: string;
    readonly name: string;

    constructor(schema: string, name: string) {
        this.schema = schema;
        this.name = name;
    }

    equal(otherTable: TableID) {
        return (
            this.schema === otherTable.schema &&
            this.name === otherTable.name
        );
    }

    toString() {
        return `${this.schema}.${this.name}`;
    }

    toStringWithoutPublic() {
        if ( this.schema === "public" ) {
            return this.name;
        }
        else {
            return `${this.schema}.${this.name}`;;
        }
    }
}