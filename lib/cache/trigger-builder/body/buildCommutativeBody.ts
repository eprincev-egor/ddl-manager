import {
    Body,
    If,
    HardCode,
    BlankLine,
    Expression,
    Declare,
    AssignVariable,
    SimpleSelect,
    AbstractAstElement
} from "../../../ast";
import { updateIf } from "./util/updateIf";
import { Update } from "../../../ast/Update";

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

export interface ICase {
    hasReferenceWithoutJoins?: Expression,
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
    oldJoins: IJoin[],
    newJoins: IJoin[],
    oldCase: ICase,
    newCase: ICase,
    deltaCase: IDeltaCase
) {
    const body = new Body({
        declares: [
            ...oldJoins.map(join =>
                new Declare({
                    name: join.variable.name,
                    type: join.variable.type
                })
            ),
            ...newJoins.map(join => 
                new Declare({
                    name: join.variable.name,
                    type: join.variable.type
                })
            )
        ],
        statements: [
            ...buildInsertOrDeleteCase(
                "DELETE",
                oldCase,
                oldJoins,
                "old"
            ),

            ...(hasMutableColumns ? [
                new If({
                    if: new HardCode({sql: "TG_OP = 'UPDATE'"}),
                    then: buildUpdateCaseBody(
                        noChanges,
                        oldJoins,
                        newJoins,
                        deltaCase
                    )
                })
            ] : []),

            ...buildInsertOrDeleteCase(
                "INSERT",
                newCase,
                newJoins,
                "new"
            )
        ]
    });
    
    return body;
}

function buildInsertOrDeleteCase(
    caseName: "INSERT" | "DELETE",
    simpleCase: ICase,
    joins: IJoin[],
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
                    ...assignVariables(joins, returnRow),
                    updateIf(
                        simpleCase.needUpdate,
                        simpleCase.update
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
    oldJoins: IJoin[],
    newJoins: IJoin[],
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

        ...assignVariables(oldJoins, "old"),
        ...reassignVariables(
            newJoins,
            oldJoins
        ),

        ...buildDeltaUpdate(deltaCase),
        
        new BlankLine(),

        updateIf(
            oldCaseCondition,
            oldUpdate
        ),

        new BlankLine(),

        updateIf(
            newCaseCondition,
            newUpdate
        ),
        new BlankLine(),
        new HardCode({sql: "return new;"})
    ];
}

function buildDeltaUpdate(deltaCase?: IDeltaCase) {
    if ( !deltaCase ) {
        return [new BlankLine()];
    }
    if ( deltaCase.needUpdate && deltaCase.needUpdate.isEmpty() ) {
        return [new BlankLine()];
    }
    if ( !deltaCase.update.set.length ) {
        return [new BlankLine()];
    }

    if ( deltaCase.needUpdate ) {
        return [new If({
            if: deltaCase.needUpdate,
            then: [
                deltaCase.update,
                new BlankLine(),
                new HardCode({sql: "return new;"})
            ]
        })];
    }
    else {
        return [
            deltaCase.update,
            new BlankLine(),
            new HardCode({sql: "return new;"})
        ]
    }
}

function doIf(
    condition: Expression | undefined,
    doBlock: AbstractAstElement[]
) {
    if ( !condition ) {
        return doBlock;
    }

    return [new If({
        if: condition,
        then: [
            ...doBlock
        ]
    })];
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

function isNotDistinctFrom(columns: string[]) {
    const conditions = columns.map(column =>
        `new.${ column } is not distinct from old.${ column }`
    );
    return Expression.and(conditions);
}