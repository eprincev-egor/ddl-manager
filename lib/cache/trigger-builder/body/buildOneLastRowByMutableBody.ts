import { 
    Body, Declare, 
    If, Expression, Select,
    HardCode, BlankLine, Update
} from "../../../ast";
import { doIf } from "./util/doIf";

export interface ILastRowParams {
    noChanges: Expression;
    isLastColumn: string;
    hasNewReference: Expression;
    hasOldReference: Expression;
    updatePrev: Update;
    updateNew: Update;
    selectPrevRowByOrder: Select;
    selectPrevRowByFlag: Select;
    updatePrevRowLastColumnTrue: Update;
    prevRowIsLess: Expression;
    updatePrevAndThisFlag: Update;
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