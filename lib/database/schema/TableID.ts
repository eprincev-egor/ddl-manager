import { DEFAULT_SCHEMA } from "../../parser/defaults";

const keywords = [
    "order",
    "where",
    "from",
    "join",
    "on",
    "select"
];
export class TableID {
    static fromString(schemaTable: string) {
        const [schema, table] = schemaTable.split(".");

        if ( !table ) {
            return new TableID(
                DEFAULT_SCHEMA,
                schema
            );
        }

        return new TableID(schema, table);
    }

    readonly schema: string;
    readonly name: string;

    constructor(schema: string, name: string) {
        this.schema = schema.replace(/"/g, "");
        this.name = name.replace(/"/g, "");
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
        const nameIsKeyWord = keywords.includes(this.name);

        if ( !nameIsKeyWord && this.schema === "public" ) {
            return this.name;
        }
        else {
            return `${this.schema}.${this.name}`;;
        }
    }
}