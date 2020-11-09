import { AbstractAstElement } from "./AbstractAstElement";
import { Body } from "./Body";

interface CreateFunctionRow {
    name: string;
    body: Body;
}

export class CreateFunction extends AbstractAstElement {

    readonly name!: string;
    readonly body!: Body;

    constructor(row: CreateFunctionRow) {
        super();
        Object.assign(this, row);
    }

    template() {
        return [
            `create or replace function ${ this.name }()`,
            "returns trigger as $body$",
            this.body.toSQL(),
            "$body$",
            "language plpgsql;"
        ];
    }
}