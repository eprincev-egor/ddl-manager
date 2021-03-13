import { 
    Body, Declare, 
    If, Expression, Select,
    AssignVariable,
    HardCode, BlankLine, Update, SimpleSelect
} from "../../../ast";
import { Exists } from "../../../ast/expression/Exists";
import { doIf } from "./util/doIf";
import { exitIf } from "./util/exitIf";

export interface ILastRowParams {
    orderVector: "asc" | "desc";
    isLastColumn: string;
    ifNeedUpdateNewOnChangeReference: Expression;
    hasNewReference: Expression;
    hasOldReference: Expression;
    hasOldReferenceAndIsLast: Expression;
    noReferenceChanges?: Expression;
    noChanges: Expression;
    updatePrev: Update;
    updateNew: Update;
    updatePrevRowLastColumnTrue: Update;
    clearLastColumnOnInsert: Update;
    selectPrevRow: Select;
    existsPrevRow: Exists;
    selectMaxPrevId: SimpleSelect;
    updateMaxRowLastColumnFalse: Update;
    updateThisRowLastColumnTrue: Update;
    exitFromDeltaUpdateIf?: Expression;
}

export function buildOneLastRowBody(ast: ILastRowParams) {
    return new Body({
        declares: [
            new Declare({
                name: "prev_row",
                type: "record"
            }),
            new Declare({
                name: "prev_id",
                type: "bigint"
            }),
            ...(ast.orderVector === "desc" ? [] : [
                new Declare({
                    name: "is_not_first",
                    type: "boolean"
                })
            ])
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
                            new If({
                                if: new HardCode({sql: `not old.${ast.isLastColumn}`}),
                                then: [
                                    new HardCode({sql: "return old;"})
                                ]
                            }),
                            new BlankLine(),
                            ast.selectPrevRow,
                            new BlankLine(),
                            new If({
                                if: new HardCode({sql: "prev_row.id is not null"}),
                                then: [ast.updatePrevRowLastColumnTrue]
                            }),
                            new BlankLine(),
                            ast.updatePrev,
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
                    ...doIf(
                        ast.noReferenceChanges,
                        [
                            ...exitIf({
                                if: ast.exitFromDeltaUpdateIf,
                                blanksAfter: [new BlankLine()]
                            }),

                            new If({
                                if: new HardCode({sql: `not new.${ast.isLastColumn}`}),
                                then: [
                                    new HardCode({sql: "return new;"})
                                ]
                            }),
                            new BlankLine(),
                            ast.updateNew,
                            new BlankLine(),
                            new HardCode({sql: "return new;"})
                        ]
                    ),
                    new BlankLine(),
                    new If({
                        if: ast.hasOldReferenceAndIsLast,
                        then: [
                            ast.selectPrevRow,
                            new BlankLine(),
                            new If({
                                if: new HardCode({sql: "prev_row.id is not null"}),
                                then: [ast.updatePrevRowLastColumnTrue]
                            }),
                            new BlankLine(),
                            ast.updatePrev
                        ]
                    }),
                    new BlankLine(),
                    new If({
                        if: ast.hasNewReference,
                        then: [
                            new AssignVariable({
                                variable: "prev_id",
                                value: ast.selectMaxPrevId
                            }),
                            new BlankLine(),
                            new If({
                                if: ast.ifNeedUpdateNewOnChangeReference,
                                then: [
                                    new If({
                                        if: new HardCode({sql: "prev_id is not null"}),
                                        then: [
                                            ast.updateMaxRowLastColumnFalse
                                        ]
                                    }),
                                    new BlankLine(),
                                    new If({
                                        if: new HardCode({sql: `not new.${ast.isLastColumn}`}),
                                        then: [
                                            ast.updateThisRowLastColumnTrue
                                        ]
                                    }),
                                    new BlankLine(),
                                    ast.updateNew
                                ]
                            })
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
                        ast.orderVector === "desc" ? [
                            ast.clearLastColumnOnInsert,
                            new BlankLine(),
                            ast.updateNew,
                            new BlankLine()
                        ] : [
                            new AssignVariable({
                                variable: "is_not_first",
                                value: ast.existsPrevRow
                            }),
                            new BlankLine(),
                            new If({
                                if: new HardCode({sql: "is_not_first"}),
                                then: [
                                    new HardCode({sql: "return new;"})
                                ]
                            }),
                            new BlankLine(),
                            ast.updateThisRowLastColumnTrue,
                            new BlankLine(),
                            ast.updateNew,
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