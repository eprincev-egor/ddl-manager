import {
    AbstractAstElement,
    Body,
    If,
    HardCode,
    BlankLine,
    Expression,
    Declare,
    AssignVariable,
    SimpleSelect
} from "../../ast";
import { isNotDistinctFrom } from "./condition/isNotDistinctFrom";
import { updateIf } from "./updateIf";
import { ICase } from "./buildCommutativeBody";

export interface IVariable {
    name: string;
    type: string;
}

export interface IJoin {
    variable: IVariable;
    table: {
        alias?: string;
        name: string;
        column: string;
    };
    on: {
        column: string;
    }
}

export interface IJoinedCase extends ICase {
    hasReference?: Expression;
    joins: IJoin[];
}

export function buildCommutativeBodyWithJoins(
    noChanges: Expression,
    oldCase: IJoinedCase,
    newCase: IJoinedCase,
    deltaCase: IJoinedCase
) {
    const body = new Body({
        declares: [
            ...oldCase.joins.map(join =>
                new Declare({
                    name: join.variable.name,
                    type: join.variable.type
                })
            ),
            ...newCase.joins.map(join => 
                new Declare({
                    name: join.variable.name,
                    type: join.variable.type
                })
            )
        ],
        statements: [
            ...buildInsertOrDeleteCase(
                "DELETE",
                "old",
                oldCase
            ),
            new If({
                if: new HardCode({sql: "TG_OP = 'UPDATE'"}),
                then: [
                    new If({
                        if: noChanges,
                        then: [
                            new HardCode({sql: "return new;"})
                        ]
                    }),
                    new BlankLine(),
                    ...assignVariables(oldCase.joins, "old"),
                    ...reassignVariables(newCase.joins, oldCase.joins),
                    new BlankLine(),

                    ...(
                        deltaCase.needUpdate ? [
                            new If({
                                if: deltaCase.needUpdate,
                                then: [
                                    new BlankLine(),
                                    deltaCase.update,
                                    new BlankLine(),
                                    new HardCode({sql: "return new;"})
                                ]
                            })
                        ] : [
                            new BlankLine(),
                            deltaCase.update,
                            new BlankLine(),
                            new HardCode({sql: "return new;"})
                        ]
                    ),

                    new BlankLine(),
                    new BlankLine(),

                    updateIf(
                        oldCase.hasReference ?
                            oldCase.hasReference.and(oldCase.needUpdate) :
                            oldCase.needUpdate,
                        oldCase.update
                    ),

                    new BlankLine(),

                    updateIf(
                        newCase.hasReference ?
                            newCase.hasReference.and(newCase.needUpdate) :
                            newCase.needUpdate,
                        newCase.update
                    ),

                    new BlankLine(),
                    new HardCode({sql: "return new;"})
                ]
            }),
            ...buildInsertOrDeleteCase(
                "INSERT",
                "new",
                newCase
            )
        ]
    });
    
    return body;
}

function buildInsertOrDeleteCase(
    caseName: "INSERT" | "DELETE",
    row: "new" | "old",
    caseData: IJoinedCase
) {
    return [
        new BlankLine(),
            new If({
                if: new HardCode({
                    sql: `TG_OP = '${caseName}'`
                }),
                then: [
                    new BlankLine(),
                    ...(caseData.hasReference ?
                        [new If({
                            if: caseData.hasReference,
                            then: [
                                new BlankLine(),

                                ...assignVariables(caseData.joins, row),

                                updateIf(
                                    caseData.needUpdate,
                                    caseData.update
                                )
                            ]
                        })] : 
                        [
                            new BlankLine(),

                            ...assignVariables(caseData.joins, row),

                            updateIf(
                                caseData.needUpdate,
                                caseData.update
                            )
                        ]),
                    new BlankLine(),
                    new HardCode({
                        sql: `return ${row};`
                    })
                ]
            }),
            new BlankLine()
    ];
}

function assignVariables(joins: IJoin[], row: "new" | "old") {
    const lines: AbstractAstElement[] = [];

    for (const join of joins) {
        lines.push(
            new If({
                if: Expression.and([
                    `${row}.${join.on.column} is not null`
                ]),
                then: [
                    new AssignVariable({
                        variable: join.variable.name,
                        value: new SimpleSelect({
                            column: join.table.column,
                            from: join.table.name,
                            where: `${row}.${join.on.column}`
                        })
                    })
                ]
            })
        );

        lines.push( new BlankLine() );
    }

    return lines;
}

function reassignVariables(newJoins: IJoin[], oldJoins: IJoin[]) {
    const lines: AbstractAstElement[] = [];

    for (let i = 0, n = newJoins.length; i < n; i++) {
        const newJoin = newJoins[i];
        const oldJoin = oldJoins[i];
        
        lines.push(new If({
            if: isNotDistinctFrom([
                newJoin.on.column
            ]),
            then: [
                new AssignVariable({
                    variable: newJoin.variable.name,
                    value: new HardCode({
                        sql: oldJoin.variable.name
                    })
                })
            ],
            else: [
                new If({
                    if: Expression.and([
                        `new.${newJoin.on.column} is not null`
                    ]),
                    then: [
                        new AssignVariable({
                            variable: newJoin.variable.name,
                            value: new SimpleSelect({
                                column: newJoin.table.column,
                                from: newJoin.table.name,
                                where: "new." + newJoin.on.column
                            })
                        })
                    ]
                })
            ]
        }));
        
        lines.push( new BlankLine() );
    }
    
    return lines;
}