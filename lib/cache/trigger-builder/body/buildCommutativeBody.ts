import {
    Body,
    If,
    HardCode,
    BlankLine,
    Expression
} from "../../../ast";
import { updateIf } from "./util/updateIf";
import { Update } from "../../../ast/Update";

export interface ICase {
    needUpdate?: Expression;
    update: Update;
}

export interface IDeltaCase extends ICase {
    old: ICase;
    new: ICase;
}

export function buildCommutativeBody(
    hasMutableColumns: boolean,
    noChanges: Expression,
    oldCase: ICase,
    newCase: ICase,
    deltaCase?: IDeltaCase
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
    deltaCase?: IDeltaCase
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
                deltaCase.needUpdate && 
                deltaCase.needUpdate.isEmpty()
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
            (deltaCase && deltaCase.old || oldCase).needUpdate,
            (deltaCase && deltaCase.old || oldCase).update
        ),

        new BlankLine(),

        updateIf(
            (deltaCase && deltaCase.new || newCase).needUpdate,
            (deltaCase && deltaCase.new || newCase).update
        ),
        new BlankLine(),
        new HardCode({sql: "return new;"})
    ];
}
