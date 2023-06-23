import {
    Body,
    If,
    HardCode,
    BlankLine,
    Expression
} from "../../../ast";
import { doIf } from "./util/doIf";
import { exitIf } from "./util/exitIf";
import { Update } from "../../../ast/Update";

export interface ICase {
    hasReferenceWithoutJoins?: Expression,
    needUpdate?: Expression;
    update: Update;
}

export interface IDeltaCase extends ICase {
    exitIf?: Expression;
    old: {
        needUpdate?: Expression;
        update?: Update;
    };
    new: {
        needUpdate?: Expression;
        update?: Update;
    };
}

export function buildCommutativeBody(
    needInsertCase: boolean,
    hasMutableColumns: boolean,
    noChanges: Expression,
    oldCase: ICase,
    newCase: ICase,
    deltaCase: IDeltaCase
) {
    const body = new Body({
        statements: [
            ...buildInsertOrDeleteCase(
                "DELETE",
                oldCase,
                "old"
            ),

            ...(hasMutableColumns ? [
                new If({
                    if: new HardCode({sql: "TG_OP = 'UPDATE'"}),
                    then: buildUpdateCaseBody(
                        noChanges,
                        deltaCase
                    )
                })
            ] : []),

            ...(needInsertCase ? buildInsertOrDeleteCase(
                "INSERT",
                newCase,
                "new"
            ): [])
        ]
    });
    
    return body;
}

function buildInsertOrDeleteCase(
    caseName: "INSERT" | "DELETE",
    simpleCase: ICase,
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

                ...doIf(simpleCase.hasReferenceWithoutJoins, [
                    ...doIf(
                        simpleCase.needUpdate,
                        [simpleCase.update]
                    )
                ]),

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
    deltaCase: IDeltaCase
) {
    const oldUpdate = deltaCase.old.update;
    const newUpdate = deltaCase.new.update;
    const oldCaseCondition = deltaCase.old.needUpdate;
    const newCaseCondition = deltaCase.new.needUpdate;

    return [
        new If({
            if: noChanges,
            then: [
                new HardCode({sql: "return new;"})
            ]
        }),
        new BlankLine(),

        ...buildDeltaUpdate(deltaCase),

        new BlankLine(),

        ...doIf(
            oldCaseCondition,
            oldUpdate ? [oldUpdate] : []
        ),

        new BlankLine(),

        ...doIf(
            newCaseCondition,
            newUpdate ? [newUpdate] : []
        ),
        new BlankLine(),
        new HardCode({sql: "return new;"})
    ];
}

function buildDeltaUpdate(deltaCase: IDeltaCase) {
    if ( !deltaCase.update.set.length ) {
        return [new BlankLine()];
    }

    if ( deltaCase.needUpdate && !deltaCase.needUpdate.isEmpty() ) {
        return [new If({
            if: deltaCase.needUpdate,
            then: [
                ...exitIf({
                    if: deltaCase.exitIf,
                    blanksAfter: [new BlankLine()]
                }),

                deltaCase.update,
                new BlankLine(),
                new HardCode({sql: "return new;"})
            ]
        })];
    }
    else {
        return [
            deltaCase.update
        ]
    }
}
