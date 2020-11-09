import { AbstractAstElement } from "./AbstractAstElement";

interface DeclareRow {
    name: string;
    type: string;
}

export class Declare extends AbstractAstElement {

    name!: string;
    type!: string;

    constructor(row: DeclareRow) {
        super();
        Object.assign(this, row);
    }

    template() {
        return [`declare ${this.name} ${this.type};`];
    }
}