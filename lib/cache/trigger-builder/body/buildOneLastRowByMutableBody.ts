import { 
    Body,
    If, Expression, Update,
    HardCode, BlankLine, Declare, Select
} from "../../../ast";
import { doIf } from "./util/doIf";
import { exitIf } from "./util/exitIf";

export interface ILastRowParams {
    noChanges: Expression;
    noReferenceChanges: Expression;
    hasSortChanges: Expression;
    newSortIsGreat: Expression;
    exitFromDeltaUpdateIf: Expression;
    hasNewReference: Expression;
    hasOldReference: Expression;
    newSortIsLessThenCacheRow: Expression;
    oldIsLast: Expression;
    newIsLast: Expression;
    updateNew: Update;
    updateNewWhereIsGreat: Update;
    updateReselectCacheRow: Update;
    updateNewWhereIsLastAndDistinctNew: Update;
    selectByOldAndLockCacheRow: Select;
    selectByNewAndLockCacheRow: Select;
}

export function buildOneLastRowByMutableBody(ast: ILastRowParams) {
    return new Body({
        declares: [new Declare({
            name: "cache_row",
            type: "record"
        })],
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
                            ast.selectByOldAndLockCacheRow,
                            new BlankLine(),
                            new If({
                                if: ast.oldIsLast,
                                then: [
                                    ast.updateReselectCacheRow
                                ]
                            })
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
                    
                    new If({
                        if: ast.noReferenceChanges,
                        then: [
                            ...exitIf({
                                if: ast.exitFromDeltaUpdateIf,
                                blanksAfter: [new BlankLine()]
                            }),

                            new If({
                                if: ast.hasSortChanges,
                                then: [
                                    ast.selectByOldAndLockCacheRow,
                                    new BlankLine(),
                                    new If({
                                        if: ast.newSortIsGreat,
                                        then: [
                                            new If({
                                                if: ast.newSortIsLessThenCacheRow,
                                                then: [
                                                    ast.updateNew
                                                ]
                                            })
                                        ],
                                        else: [
                                            new If({
                                                if: ast.newIsLast,
                                                then: [
                                                    ast.updateReselectCacheRow
                                                ]
                                            })
                                        ]
                                    })
                                ],
                                else: [
                                    ast.updateNewWhereIsLastAndDistinctNew
                                ]
                            }),

                            new BlankLine(),
                            new HardCode({sql: "return new;"})
                        ]
                    }),
                    
                    new BlankLine(),
                    new If({
                        if: ast.hasOldReference,
                        then: [
                            ast.selectByOldAndLockCacheRow,
                            new BlankLine(),
                            new If({
                                if: ast.oldIsLast,
                                then: [
                                    ast.updateReselectCacheRow
                                ]
                            })
                        ]
                    }),
                    new BlankLine(),

                    new If({
                        if: ast.hasNewReference,
                        then: [
                            ast.selectByNewAndLockCacheRow,
                            new BlankLine(),
                            ast.updateNewWhereIsGreat
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
                            ast.selectByNewAndLockCacheRow,
                            new BlankLine(),
                            ast.updateNewWhereIsGreat
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