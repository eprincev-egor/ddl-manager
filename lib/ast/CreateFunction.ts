import { AbstractAstElement } from "./AbstractAstElement";
import { Body } from "./Body";
import { DatabaseFunction } from "./DatabaseFunction";

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

    toDatabaseFunction(): DatabaseFunction {
        return new DatabaseFunction({
            schema: "public",
            name: this.name,
            body: this.body.toSQL(),
            comment: "cache",
            args: [],
            returns: {type: "trigger"}
        });
    }
}