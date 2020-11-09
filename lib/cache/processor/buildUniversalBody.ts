import {
    AssignVariable,
    BlankLine,
    Body,
    Expression,
    Declare,
    HardCode,
    If,
    InlineSelectValues,
    SetSelectItem,
    Update,
    With,
    WithQuery,
    Table,
    TableReference
} from "../../ast";

export interface IUniversalAST {
    forTable: string;
    triggerTable: Table;
    from: string[];
    where?: Expression;
    select: string;
    updateColumns: string[];
    triggerTableColumns: string[];
}

export function buildUniversalBody(ast: IUniversalAST) {

    return new Body({
        declares: [
            new Declare({
                name: "new_row",
                type: "record"
            }),
            new Declare({
                name: "old_row",
                type: "record"
            }),
            new Declare({
                name: "return_row",
                type: "record"
            })
        ],
        statements: [
            new If({
                if: Expression.and([
                    "TG_OP = 'DELETE'"
                ]),
                then: [
                    new AssignVariable({
                        variable: "return_row",
                        value: new HardCode({
                            sql: "old"
                        })
                    })
                ],
                else: [
                    new AssignVariable({
                        variable: "return_row",
                        value: new HardCode({
                            sql: "new"
                        })
                    })
                ]
            }),
            new BlankLine(),
            new AssignVariable({
                variable: "new_row",
                value: new HardCode({
                    sql: "return_row"
                })
            }),
            new AssignVariable({
                variable: "old_row",
                value: new HardCode({
                    sql: "return_row"
                })
            }),
            new BlankLine(),
            new If({
                if: Expression.and([
                    "TG_OP in ('INSERT', 'UPDATE')"
                ]),
                then: [
                    new AssignVariable({
                        variable: "new_row",
                        value: new HardCode({
                            sql: "new"
                        })
                    })
                ]
            }),
            new If({
                if: Expression.and([
                    "TG_OP in ('UPDATE', 'DELETE')"
                ]),
                then: [
                    new AssignVariable({
                        variable: "old_row",
                        value: new HardCode({
                            sql: "old"
                        })
                    })
                ]
            }),
            new BlankLine(),
            new Update({
                with: new With({
                    queries: [
                        new WithQuery({
                            name: "changed_rows",
                            select: new InlineSelectValues({
                                values: ast.triggerTableColumns.map(column =>
                                    `old_row.${column}`
                                ),
                                union: new InlineSelectValues({
                                    values: ast.triggerTableColumns.map(column =>
                                        `new_row.${column}`
                                    )
                                })
                            })
                        })
                    ]
                }),
                table: ast.forTable,
                set: [new SetSelectItem({
                    columns: ast.updateColumns,
                    select: ast.select
                })],
                from: ast.from.map(fromTable => {
                    if ( fromTable === ast.triggerTable.toStringWithoutPublic() ) {
                        return "changed_rows";
                    }
                    return fromTable;
                }),
                where: ast.where ?
                    ast.where.replaceTable(
                        ast.triggerTable,
                        new TableReference(
                            ast.triggerTable,
                            "changed_rows"
                        )
                    ) : 
                    undefined
            }),
            new BlankLine(),
            new HardCode({
                sql: "return return_row;"
            })
        ]
    })
}