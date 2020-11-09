import { AbstractAstElement } from "./AbstractAstElement";
import { CreateFunction } from "./CreateFunction";

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
}