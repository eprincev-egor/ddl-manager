import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { ColumnReference } from "./ColumnReference";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { Spaces } from "../Spaces";
import { IExpressionElement } from "./interface";
import { escapeRegExp } from "lodash";

export interface IUnknownSyntax {
    toString(): string;
    clone(): IUnknownSyntax;
}

export interface IColumnsMap {
    [column: string]: IExpressionElement;
}

export class UnknownExpressionElement extends AbstractExpressionElement {

    static fromSql(sql: string, columnsMap: IColumnsMap = {}) {
        const syntax = () => ({
            toString: () => sql,
            clone: () => syntax()
        });
        return new UnknownExpressionElement(syntax(), columnsMap);
    }

    private syntax: IUnknownSyntax;
    readonly columnsMap: IColumnsMap;
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
        replaceTable: TableReference | TableID,
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

    replaceColumn(replaceColumn: ColumnReference, toSql: IExpressionElement) {
        
        const newColumnsMap = {...this.columnsMap};

        for (const column in this.columnsMap) {
            const columnReference = this.columnsMap[ column ];

            if ( columnReference instanceof ColumnReference ) {
                if ( columnReference.equal(replaceColumn) ) {
                    newColumnsMap[ column ] = toSql;
                }
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
                new RegExp(escapeRegExp(column), "g"),
                columnReference.toString()
            );
        }

        const lines = sql.split("\n");
        if ( lines.length === 1 ) {
            return [lines[0].trim()];
        }

        return [
            lines[0],
            ...lines.slice(1).map(line => 
                line.trim() ? spaces + line : ""
            )
        ];
    }
}