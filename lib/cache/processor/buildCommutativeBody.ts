import {
    Body,
    If,
    HardCode,
    BlankLine,
    Expression
} from "../../ast";
import { updateIf } from "./updateIf";
import { Update } from "../../ast/Update";

export interface ICase {
    needUpdate?: Expression;
    update: Update;
}

export function buildCommutativeBody(
    hasMutableColumns: boolean,
    noChanges: Expression,
    oldCase: ICase,
    newCase: ICase,
    deltaCase?: ICase
) {
    const body = new Body({
        statements: [
            ...buildInsertOrDeleteCase(
                "DELETE",
                oldCase.needUpdate,
                oldCase.update,
                "old"
            ),

            ...(hasMutableColumns ? [
                new If({
                    if: new HardCode({sql: "TG_OP = 'UPDATE'"}),
                    then: buildUpdateCaseBody(
                        noChanges,
                        oldCase,
                        newCase,
                        deltaCase
                    )
                })
            ] : []),

            ...buildInsertOrDeleteCase(
                "INSERT",
                newCase.needUpdate,
                newCase.update,
                "new"
            )
        ]
    });
    
    return body;
}

function buildInsertOrDeleteCase(
    caseName: "INSERT" | "DELETE",
    needUpdate: Expression | undefined,
    update: Update,
    returnRow: "new" | "old"
) {
    return [
        new BlankLine(),
        new If({
            if: new HardCode({
                sql: `TG_OP = '${caseName}'`
            }),
            then: [
                new BlankLine(),
                updateIf(
                    needUpdate,
                    update
                ),
                new BlankLine(),
                new HardCode({
                    sql: `return ${returnRow};`
                })
            ]
        }),
        new BlankLine()
    ];
}

function buildUpdateCaseBody(
    noChanges: Expression,
    oldCase: ICase,
    newCase: ICase,
    deltaCase?: ICase
) {
    return [
        new If({
            if: noChanges,
            then: [
                new HardCode({sql: "return new;"})
            ]
        }),
        new BlankLine(),

        // TODO: refactor sub ternary
        ...(
            (
                !deltaCase ||
                deltaCase.needUpdate && deltaCase.needUpdate.isEmpty()
            ) ? [
                new BlankLine()
            ] : (
                deltaCase.needUpdate ?
                    [new If({
                        if: deltaCase.needUpdate,
                        then: [
                            deltaCase.update,
                            new BlankLine(),
                            new HardCode({sql: "return new;"})
                        ]
                    })] : [
                        deltaCase.update,
                        new BlankLine(),
                        new HardCode({sql: "return new;"})
                    ]
            )
        ),
        
        new BlankLine(),

        updateIf(
            oldCase.needUpdate,
            oldCase.update
        ),

        new BlankLine(),

        updateIf(
            newCase.needUpdate,
            newCase.update
        ),
        new BlankLine(),
        new HardCode({sql: "return new;"})
    ];
}
