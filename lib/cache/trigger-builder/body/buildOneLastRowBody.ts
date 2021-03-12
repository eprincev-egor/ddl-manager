import { 
    Body, Declare, 
    If, Expression, Select,
    AssignVariable,
    HardCode, BlankLine, Update, SimpleSelect
} from "../../../ast";
import { doIf } from "./util/doIf";

export interface ILastRowParams {
    isLastColumn: string;
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
    selectMaxPrevId: SimpleSelect;
    updateMaxRowLastColumnFalse: Update;
    updateThisRowLastColumnTrue: Update;
}

export function buildOneLastRowBody(ast: ILastRowParams) {
    return new Body({
        declares: [
            new Declare({
                name: "prev_row",
                type: "record"
            }),
            new Declare({
                name: "max_prev_id",
                type: "bigint"
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
                                variable: "max_prev_id",
                                value: ast.selectMaxPrevId
                            }),
                            new BlankLine(),
                            new If({
                                if: Expression.or([
                                    "max_prev_id < new.id",
                                    "max_prev_id is null"
                                ]),
                                then: [
                                    new If({
                                        if: new HardCode({sql: "max_prev_id is not null"}),
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
                        [
                            ast.clearLastColumnOnInsert,
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