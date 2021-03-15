import { 
    Body, Declare, 
    If, Expression, Select,
    HardCode, BlankLine, Update
} from "../../../ast";
import { doIf } from "./util/doIf";
import { exitIf } from "./util/exitIf";

export interface ILastRowParams {
    noChanges: Expression;
    noReferenceAndSortChanges: Expression;
    noReferenceChanges: Expression;
    isLastAndHasDataChange: Expression;
    isLastAndSortMinus: Expression;
    prevRowIsGreat: Expression;
    exitFromDeltaUpdateIf: Expression;
    isLastColumn: string;
    hasNewReference: Expression;
    hasOldReference: Expression;
    updatePrev: Update;
    updateNew: Update;
    selectPrevRowByOrder: Select;
    selectPrevRowByFlag: Select;
    selectPrevRowWhereGreatOrder: Select;
    updatePrevRowLastColumnTrue: Update;
    updateThisRowLastColumnFalse: Update;
    prevRowIsLess: Expression;
    updatePrevAndThisFlag: Update;
    updateMaxRowLastColumnFalse: Update;
    updateThisRowLastColumnTrue: Update;
    updatePrevAndThisFlagNot: Update;
    hasOldReferenceAndIsLast: Expression;
    isNotLastAndSortPlus: Expression;
}

export function buildOneLastRowByMutableBody(ast: ILastRowParams) {
    return new Body({
        declares: [
            new Declare({
                name: "prev_row",
                type: "record"
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
                            ast.selectPrevRowByOrder,
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
                        ast.noReferenceAndSortChanges,
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
                        if: ast.noReferenceChanges,
                        then: [
                            ...exitIf({
                                if: ast.exitFromDeltaUpdateIf,
                                blanksAfter: [new BlankLine()]
                            }),

                            new If({
                                if: ast.isNotLastAndSortPlus,
                                then: [
                                    ast.selectPrevRowByFlag,
                                    new BlankLine(),
                                    new If({
                                        if: ast.prevRowIsLess,
                                        then: [
                                            ast.updatePrevAndThisFlag,
                                            new BlankLine(),
                                            ast.updateNew,
                                            new BlankLine(),
                                            new HardCode({sql: "return new;"})
                                        ]
                                    })
                                ]
                            }),
                            new BlankLine(),
                            
                            new If({
                                if: ast.isLastAndSortMinus,
                                then: [
                                    ast.selectPrevRowWhereGreatOrder,
                                    new BlankLine(),
                                    new If({
                                        if: Expression.and([
                                            "prev_row.id is not null",
                                            ast.prevRowIsGreat
                                        ]),
                                        then: [
                                            ast.updatePrevAndThisFlagNot,
                                            new BlankLine(),
                                            ast.updatePrev,
                                            new BlankLine(),
                                            new HardCode({sql: "return new;"})
                                        ]
                                    })
                                ]
                            }),
                            new BlankLine(),
                            
                            new If({
                                if: ast.isLastAndHasDataChange,
                                then: [
                                    ast.updateNew
                                ]
                            }),
                            new BlankLine(),
                            new HardCode({sql: "return new;"})
                        ]
                    }),
                    
                    new BlankLine(),
                    new If({
                        if: ast.hasOldReferenceAndIsLast,
                        then: [
                            ast.selectPrevRowByOrder,
                            new BlankLine(),
                            new If({
                                if: new HardCode({sql: "prev_row.id is not null"}),
                                then: [ast.updatePrevRowLastColumnTrue]
                            }),
                            new BlankLine(),
                            new If({
                                if: ast.exitFromDeltaUpdateIf,
                                then: [
                                    ast.updateThisRowLastColumnFalse
                                ]
                            }),
                            new BlankLine(),
                            ast.updatePrev
                        ]
                    }),
                    new BlankLine(),

                    new If({
                        if: ast.hasNewReference,
                        then: [
                            ast.selectPrevRowByFlag,
                            new BlankLine(),
                            new If({
                                if: ast.prevRowIsLess,
                                then: [
                                    new If({
                                        if: new HardCode({sql: "prev_row.id is not null"}),
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
                            new BlankLine(),
                            ast.selectPrevRowByFlag,
                            new BlankLine(),
                            new If({
                                if: ast.prevRowIsLess,
                                then: [
                                    ast.updatePrevAndThisFlag,
                                    new BlankLine(),
                                    ast.updateNew
                                ]
                            })
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