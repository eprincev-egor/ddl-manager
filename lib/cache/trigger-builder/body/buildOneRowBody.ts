import {
    Expression, Update,
    Body, If,
    HardCode, BlankLine
} from "../../../ast";

export interface IUpdateCase {
    needUpdate: Expression;
    update: Update;
}

export interface IOneAst {
    onDelete: IUpdateCase;
    onUpdate: {
        noChanges: Expression;
        update: Update;
    };
    onInsert: IUpdateCase;
}

export function buildOneRowBody(ast: IOneAst) {
    return new Body({
        declares: [],
        statements: [
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
                    ast.onUpdate.update,
                    new BlankLine(),
                    new HardCode({
                        sql: `return new;`
                    })
                ]
            }),
            new BlankLine(),
            new If({
                if: Expression.and([
                    "TG_OP = 'INSERT'"
                ]),
                then: [
                    new If({
                        if: ast.onInsert.needUpdate,
                        then: [
                            ast.onInsert.update
                        ]
                    }),
                    new BlankLine(),
                    new HardCode({
                        sql: `return new;`
                    })
                ]
            }),
        ]
    })
}