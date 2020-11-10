import { AbstractAstElement } from "./AbstractAstElement";
import { CreateFunction } from "./CreateFunction";
import { DatabaseTrigger } from "./DatabaseTrigger";

interface CreateTriggerRow {
    table: string;
    columns: string[];
    function: CreateFunction;
}

export class CreateTrigger extends AbstractAstElement {

    readonly table!: string;
    readonly columns!: string[];
    readonly function!: CreateFunction;

    constructor(row: CreateTriggerRow) {
        super();
        Object.assign(this, row);
    }

    template() {
        return [
            this.function.toSQL(),
            "",
            `create trigger ${ this.function.name }`,
            `after insert or${ 
                this.columns.length ? 
                    ` update of ${ this.columns.join(", ") } or` : 
                    "" 
            } delete`,
            `on ${ this.table }`,
            "for each row",
            `execute procedure ${ this.function.name }();`
        ];
    }

    toDatabaseTrigger(): DatabaseTrigger {
        return new DatabaseTrigger({
            name: this.function.name,
            insert: true,
            delete: true,
            after: true,
            updateOf: this.columns,
            update: this.columns.length === 0 ? true : undefined,
            procedure: {
                schema: "public",
                name: this.function.name,
                args: []
            },
            table: {
                schema: this.table.split(".")[0],
                name: this.table.split(".")[1]
            },
            comment: "cache"
        });
    }
}