import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { ColumnReference } from "./ColumnReference";
import { Table } from "../Table";
import { TableReference } from "../TableReference";
import { Spaces } from "../Spaces";

export interface IUnknownSyntax {
    toString(): string;
    clone(): IUnknownSyntax;
}

export interface IColumnsMap {
    [column: string]: ColumnReference | string;
}

export class UnknownExpressionElement extends AbstractExpressionElement {

    static fromSql(sql: string) {
        const syntax = () => ({
            toString: () => sql,
            clone: () => syntax()
        });
        return new UnknownExpressionElement(syntax());
    }

    private syntax: IUnknownSyntax;
    private columnsMap: IColumnsMap;
    constructor(syntax: IUnknownSyntax, columnsMap: IColumnsMap = {}) {
        super();
        this.syntax = syntax;
        this.columnsMap = columnsMap;
    }

    protected children() {
        return Object.values(this.columnsMap)
            .filter(columnRef => 
                columnRef instanceof ColumnReference
            ) as ColumnReference[];
    }

    replaceTable(
        replaceTable: TableReference | Table,
        toTable: TableReference
    ) {
        const newColumnsMap = {...this.columnsMap};

        for (const column in this.columnsMap) {
            const columnReference = this.columnsMap[ column ];

            if ( typeof columnReference === "string" ) {
                continue;
            }

            newColumnsMap[ column ] = columnReference.replaceTable(replaceTable, toTable); 
        }

        return this.clone(newColumnsMap);
    }

    replaceColumn(replaceColumn: string, toSql: string) {
        
        const newColumnsMap = {...this.columnsMap};

        for (const column in this.columnsMap) {
            const columnReference = this.columnsMap[ column ];

            if ( columnReference.toString() === replaceColumn ) {
                newColumnsMap[ column ] = toSql;
            }
        }

        return this.clone(newColumnsMap);
    }

    clone(columnsMap?: IColumnsMap) {
        return new UnknownExpressionElement(
            this.syntax.clone(),
            columnsMap || Object.assign({}, this.columnsMap)
        );
    }

    template(spaces: Spaces) {
        let sql = this.syntax.toString();

        for (const column in this.columnsMap) {
            const columnReference = this.columnsMap[ column ];
            sql = sql.replace(
                new RegExp(column, "g"),
                columnReference.toString()
            );
        }

        const lines = sql.split("\n").map(line =>
            spaces + line
        );
        if ( lines.length === 1 ) {
            return [lines[0].trim()];
        }

        return lines;
    }
}