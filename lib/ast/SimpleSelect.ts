import { AbstractAstElement } from "./AbstractAstElement";
import { Spaces } from "./Spaces";

interface SimpleSelectRow {
    columns: string[];
    into?: string[];
    from: string;
    where: string;
}

export class SimpleSelect extends AbstractAstElement {

    readonly columns!: string[];
    readonly from!: string;
    readonly where!: string;
    readonly into!: string[];

    constructor(row: SimpleSelectRow) {
        super();
        Object.assign(this, row);
        this.into = row.into || [];
    }

    template(spaces: Spaces) {
        const originalSpaces = spaces;
        if ( !this.into.length ) {
            spaces = spaces.plusOneLevel();
        }

        const template = [
            spaces + "select",

            ...this.columnsTemplate(spaces),

            ...this.intoTemplate(spaces),
            
            spaces + `from ${this.from}`,
            spaces + "where",
            spaces.plusOneLevel() + `${this.from}.id = ${this.where}`
        ];

        if ( !this.into.length ) {
            template.unshift(originalSpaces + "(");
            template.push(originalSpaces + ")");
        }
        else {
            template[ template.length - 1 ] += ";";
        }

        return template;
    }

    private columnsTemplate(spaces: Spaces) {
        const selectColumnsTemplate: string[] = [];

        this.columns.forEach((columnName, i) => {
            const selectColumn = `${this.from}.${columnName}`;
            const comma = i === this.columns.length - 1 ? "" : ",";

            selectColumnsTemplate.push(
                spaces.plusOneLevel() +
                selectColumn +
                comma
            );
        });

        return selectColumnsTemplate;
    }

    private intoTemplate(spaces: Spaces) {
        if ( !this.into.length ) {
            return [];
        }

        const intoTemplate: string[] = [
            spaces + "into"
        ];

        this.into.forEach((varName, i) => {
            const comma = i === this.columns.length - 1 ? "" : ",";

            intoTemplate.push(
                spaces.plusOneLevel() + 
                varName + 
                comma
            );
        });

        return intoTemplate;
    }
}