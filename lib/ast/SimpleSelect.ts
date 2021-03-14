import { TableID } from "../database/schema/TableID";
import { AbstractAstElement } from "./AbstractAstElement";
import { Spaces } from "./Spaces";
import { Expression } from "./expression/Expression";
import { TableReference } from "../database/schema/TableReference";

interface SimpleSelectRow {
    columns: string[];
    into?: string[];
    from: TableID | TableReference;
    where: Expression | string;
}

export class SimpleSelect extends AbstractAstElement {

    readonly columns!: string[];
    readonly from!: TableID | TableReference;
    readonly where!: Expression | string;
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
            
            spaces + `from ${this.fromClause()}`,
            spaces + "where",

            (
                this.where instanceof Expression ?
                    this.where.toSQL(
                        spaces.plusOneLevel()
                    ) :
                    spaces.plusOneLevel() + 
                    `${this.getFromIdentifier()}.id = ${this.where}`
            )
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
            const comma = i === this.columns.length - 1 ? "" : ",";

            const selectColumn = /^\w+$/.test(columnName) ?
                `${this.getFromIdentifier()}.${columnName}` :
                columnName;

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

    private getFromIdentifier() {
        if ( this.from instanceof TableID ) {
            return this.from.toStringWithoutPublic();
        }
        else {
            return this.from.getIdentifier();
        }
    }

    private fromClause() {
        if ( this.from instanceof TableID ) {
            return this.from.toStringWithoutPublic();
        }
        else {
            return this.from.toString();
        }
    }
}