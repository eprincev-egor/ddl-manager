
export class TableID {
    static fromString(schemaTable: string) {
        const [schema, table] = schemaTable.split(".");

        if ( !table ) {
            return new TableID(
                "public",
                schema
            );
        }

        return new TableID(schema, table);
    }

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

    // TODO: use it as default toString
    toStringWithoutPublic() {
        if ( this.schema === "public" ) {
            return this.name;
        }
        else {
            return `${this.schema}.${this.name}`;;
        }
    }
}