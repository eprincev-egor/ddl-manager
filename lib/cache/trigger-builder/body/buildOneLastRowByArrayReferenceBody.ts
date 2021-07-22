import { 
    Body, Declare, 
    If, Expression,
    AssignVariable,
    HardCode, BlankLine, Update, AbstractAstElement
} from "../../../ast";
import { Column } from "../../../database/schema/Column";
import { doIf } from "./util/doIf";

export interface ILastRowParams {
    needMatching: boolean;
    orderByColumnName: string;
    dataFields: string[];
    arrColumn: Column;
    hasNewReference: Expression;
    hasOldReference: Expression;
    noChanges: Expression;
    noOrderChanges: Expression;
    newSortIsGreat: Expression;
    updateOnDelete: Update;
    updateOnInsert: Update;
    updateNotChangedIds: Update;
    updateNotChangedIdsWithReselect: Update;
    updateNotChangedIdsWhereSortIsLess: Update;
    updateDeletedIds: Update;
    updateInsertedIds: Update;
    matchedOld: AbstractAstElement;
    matchedNew: AbstractAstElement;
}

export function buildOneLastRowByArrayReferenceBody(ast: ILastRowParams) {
    return new Body({
        declares: [
            ...(
                ast.needMatching ? [
                    new Declare({
                        name: "matched_old",
                        type: "boolean"
                    }),
                    new Declare({
                        name: "matched_new",
                        type: "boolean"
                    }),
                ] : []
            ),
            new Declare({
                name: "inserted_" + ast.arrColumn.name,
                type: ast.arrColumn.type.toString()
            }),
            new Declare({
                name: "not_changed_" + ast.arrColumn.name,
                type: ast.arrColumn.type.toString()
            }),
            new Declare({
                name: "deleted_" + ast.arrColumn.name,
                type: ast.arrColumn.type.toString()
            })
        ],
        statements: [
            new BlankLine(),
            new If({
                if: Expression.and([
                    "TG_OP = 'DELETE'"
                ]),
                then: [
                    ...doIf(
                        ast.hasOldReference,
                        [
                            ast.updateOnDelete,
                            new BlankLine()
                        ]
                    ),
                    new BlankLine(),
                    new HardCode({sql: "return old;"})
                ]
            }),
            new BlankLine(),
            new If({
                if: Expression.and([
                    "TG_OP = 'UPDATE'"
                ]),
                then: [
                    new If({
                        if: ast.noChanges,
                        then: [
                            new HardCode({
                                sql: `return new;`
                            })
                        ]
                    }),
                    new BlankLine(),

                    ...(ast.needMatching ? [
                        new AssignVariable({
                            variable: "matched_old",
                            value: ast.matchedOld
                        }),
                        new AssignVariable({
                            variable: "matched_new",
                            value: ast.matchedNew
                        }),
                        new BlankLine(),
                        new If({
                            if: Expression.and([
                                "not matched_old",
                                "not matched_new"
                            ]),
                            then: [
                                new HardCode({sql: "return new;"})
                            ]
                        }),
                        new BlankLine(),
                        new If({
                            if: Expression.and([
                                "matched_old",
                                "not matched_new"
                            ]),
                            then: [
                                new AssignVariable({
                                    variable: "inserted_" + ast.arrColumn.name,
                                    value: Expression.unknown("null")
                                }),
                                new AssignVariable({
                                    variable: "not_changed_" + ast.arrColumn.name,
                                    value: Expression.unknown("null")
                                }),
                                new AssignVariable({
                                    variable: "deleted_" + ast.arrColumn.name,
                                    value: Expression.unknown(`old.${ ast.arrColumn.name }`)
                                })
                            ]
                        }),
                        new BlankLine(),
                        new If({
                            if: Expression.and([
                                "not matched_old",
                                "matched_new"
                            ]),
                            then: [
                                new AssignVariable({
                                    variable: "inserted_" + ast.arrColumn.name,
                                    value: Expression.unknown(`new.${ ast.arrColumn.name }`)
                                }),
                                new AssignVariable({
                                    variable: "not_changed_" + ast.arrColumn.name,
                                    value: Expression.unknown("null")
                                }),
                                new AssignVariable({
                                    variable: "deleted_" + ast.arrColumn.name,
                                    value: Expression.unknown("null")
                                })
                            ]
                        }),
    
                        new BlankLine(),
                        new If({
                            if: Expression.and([
                                "matched_old",
                                "matched_new"
                            ]),
                            then: [
                                new AssignVariable({
                                    variable: "inserted_" + ast.arrColumn.name,
                                    value: Expression.unknown(
                                        `cm_get_inserted_elements(old.${ast.arrColumn.name}, new.${ast.arrColumn.name})`
                                    )
                                }),
                                new AssignVariable({
                                    variable: "not_changed_" + ast.arrColumn.name,
                                    value: Expression.unknown(
                                        `cm_get_not_changed_elements(old.${ast.arrColumn.name}, new.${ast.arrColumn.name})`
                                    )
                                }),
                                new AssignVariable({
                                    variable: "deleted_" + ast.arrColumn.name,
                                    value: Expression.unknown(
                                        `cm_get_deleted_elements(old.${ast.arrColumn.name}, new.${ast.arrColumn.name})`
                                    )
                                })
                            ]
                        })
                    ] : [
                        new AssignVariable({
                            variable: "inserted_" + ast.arrColumn.name,
                            value: Expression.unknown(
                                `cm_get_inserted_elements(old.${ast.arrColumn.name}, new.${ast.arrColumn.name})`
                            )
                        }),
                        new AssignVariable({
                            variable: "not_changed_" + ast.arrColumn.name,
                            value: Expression.unknown(
                                `cm_get_not_changed_elements(old.${ast.arrColumn.name}, new.${ast.arrColumn.name})`
                            )
                        }),
                        new AssignVariable({
                            variable: "deleted_" + ast.arrColumn.name,
                            value: Expression.unknown(
                                `cm_get_deleted_elements(old.${ast.arrColumn.name}, new.${ast.arrColumn.name})`
                            )
                        })
                    ]),
                    
                    new BlankLine(),
                    new BlankLine(),
                    new If({
                        if: Expression.and([
                            `not_changed_${ast.arrColumn.name} is not null`
                        ]),
                        then: ast.orderByColumnName === "id" ? [
                            new If({
                                if: Expression.or(
                                    ast.dataFields.map(columnName =>
                                        `new.${columnName} is distinct from old.${columnName}`
                                    )
                                ),
                                then: [
                                    ast.updateNotChangedIds
                                ]
                            })
                        ] : [
                            new If({
                                if: ast.noOrderChanges,
                                then: [
                                    new If({
                                        if: Expression.or(
                                            ast.dataFields.map(columnName =>
                                                `new.${columnName} is distinct from old.${columnName}`
                                            )
                                        ),
                                        then: [
                                            ast.updateNotChangedIds
                                        ]
                                    })
                                ],
                                else: [
                                    new If({
                                        if: ast.newSortIsGreat,
                                        then: [
                                            ast.updateNotChangedIdsWhereSortIsLess
                                        ],
                                        else: [
                                            ast.updateNotChangedIdsWithReselect
                                        ]
                                    })
                                ]
                            })
                        ]
                    }),
                    new BlankLine(),
                    new If({
                        if: Expression.unknown(
                            `deleted_${ast.arrColumn.name} is not null`
                        ),
                        then: [
                            ast.updateDeletedIds
                        ]
                    }),
                    new BlankLine(),
                    new If({
                        if: Expression.unknown(
                            `inserted_${ast.arrColumn.name} is not null`
                        ),
                        then: [
                            ast.updateInsertedIds
                        ]
                    }),
                    new BlankLine(),
                    new HardCode({sql: "return new;"})
                ]
            }),
            new BlankLine(),
            new If({
                if: Expression.and([
                    "TG_OP = 'INSERT'"
                ]),
                then: [
                    ...doIf(
                        ast.hasNewReference,
                        [
                            ast.updateOnInsert,
                            new BlankLine()
                        ]
                    ),
                    new BlankLine(),
                    new HardCode({sql: "return new;"})
                ]
            }),
            new BlankLine()
        ]
    })
}