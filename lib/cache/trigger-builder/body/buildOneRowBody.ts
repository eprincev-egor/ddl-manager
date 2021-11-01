import {
    Expression, Update,
    Body, If,
    HardCode, BlankLine, Declare, SimpleSelect, AbstractAstElement
} from "../../../ast";
import { TableID } from "../../../database/schema/TableID";
import { doIf } from "./util/doIf";

export interface IUpdateCase {
    needUpdate: Expression;
    update: Update;
}

export interface ISelectRecord {
    recordName: string;
    select: string[];
    from: TableID;
    where: string;
}

export interface IOneAst {
    selects: ISelectRecord[];
    onDelete: IUpdateCase;
    onUpdate: {
        needUpdate?: Expression;
        noChanges: Expression;
        update: Update;
    };
    onInsert?: IUpdateCase;
}

export function buildOneRowBody(ast: IOneAst) {
    return new Body({
        declares: ast.selects.map(select =>
            new Declare({
                name: select.recordName,
                type: "record"
            })
        ),
        statements: [
            new BlankLine(),
            new If({
                if: Expression.and([
                    "TG_OP = 'DELETE'"
                ]),
                then: [
                    new If({
                        if: ast.onDelete.needUpdate,
                        then: [
                            ast.onDelete.update
                        ]
                    }),
                    new BlankLine(),
                    new HardCode({
                        sql: `return old;`
                    })
                ]
            }),
            new BlankLine(),
            new If({
                if: Expression.and([
                    "TG_OP = 'UPDATE'"
                ]),
                then: [
                    new If({
                        if: ast.onUpdate.noChanges,
                        then: [
                            new HardCode({
                                sql: `return new;`
                            })
                        ]
                    }),
                    new BlankLine(),
                    ...doIf(
                        ast.onUpdate.needUpdate, [
                            ...assignJoinedRows(ast.selects),
                            ast.onUpdate.update
                        ]
                    ),
                    new BlankLine(),
                    new HardCode({
                        sql: `return new;`
                    })
                ]
            }),
            ...(ast.onInsert ? [
                new BlankLine(),
                new If({
                    if: Expression.and([
                        "TG_OP = 'INSERT'"
                    ]),
                    then: [
                        new If({
                            if: ast.onInsert.needUpdate,
                            then: [
                                ...assignJoinedRows(ast.selects),
                                ast.onInsert.update
                            ]
                        }),
                        new BlankLine(),
                        new HardCode({
                            sql: `return new;`
                        })
                    ]
                })
            ] : []),
            new BlankLine()
        ]
    })
}

function assignJoinedRows(selects: ISelectRecord[]): AbstractAstElement[] {
    if ( selects.length === 0 ) {
        return [];
    }

    const output: AbstractAstElement[] = [
        new BlankLine()
    ];
    for (const select of selects) {
        output.push(...assignJoinedRow(select));
    }
    return output;
}

function assignJoinedRow(select: ISelectRecord): AbstractAstElement[] {
    return [
        new SimpleSelect({
            columns: select.select,
            from: select.from,
            into: [select.recordName],
            where: Expression.unknown(`${select.from}.id = new.${select.where}`)
        }),
        new BlankLine()
    ]
}