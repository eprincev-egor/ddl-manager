import {
    Expression, Update,
    Body, If,
    HardCode, BlankLine
} from "../../../ast";
import { updateIf } from "./util/updateIf";

export interface IUpdateCase {
    needUpdate: Expression;
    update: Update;
}

export interface IOneAst {
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
        declares: [],
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
                    ...updateIf(
                        ast.onUpdate.needUpdate,
                        ast.onUpdate.update
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