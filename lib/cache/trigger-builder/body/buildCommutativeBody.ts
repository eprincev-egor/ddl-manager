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

    const combinedJoins = groupJoinsByTableAndFilter(joins);

    for (const combinedJoin of combinedJoins) {
        lines.push(
            new If({
                if: Expression.and([
                    `${row}.${combinedJoin.byColumn} is not null`
                ]),
                then: assignCombinedJoinVariables(
                    combinedJoin,
                    row
                )
            })
        );

        lines.push( new BlankLine() );
    }

    return lines;
}

function assignCombinedJoinVariables(
    combinedJoin: ICombinedJoin,
    row: "new" | "old"
) {
    if ( combinedJoin.variables.length === 1 ) {
        return [
            new AssignVariable({
                variable: combinedJoin.variables[0],
                value: new SimpleSelect({
                    columns: combinedJoin.joinedColumns,
                    from: combinedJoin.joinedTable,
                    where: `${row}.${combinedJoin.byColumn}`
                })
            })
        ]
    }
    
    return [
        new SimpleSelect({
            columns: combinedJoin.joinedColumns,
            into: combinedJoin.variables,
            from: combinedJoin.joinedTable,
            where: `${row}.${combinedJoin.byColumn}`
        })
    ];
}

interface ICombinedJoin {
    variables: string[];
    joinedColumns: string[];
    joinedTable: string;
    byColumn: string;
}
function groupJoinsByTableAndFilter(joins: IJoin[]) {
    const combinedJoinByKey: {[key: string]: ICombinedJoin} = {};

    for (const join of joins) {
        const key = join.table.name + ":" + join.on.column;

        const combinedJoin: ICombinedJoin = combinedJoinByKey[ key ] || {
            variables: [],
            joinedColumns: [],
            joinedTable: join.table.name,
            byColumn: join.on.column
        };

        combinedJoin.variables.push(
            join.variable.name
        );
        combinedJoin.joinedColumns.push(
            join.table.column
        );
        combinedJoinByKey[ key ] = combinedJoin;
    }

    const combinedJoins = Object.values(combinedJoinByKey);
    return combinedJoins;
}

function reassignVariables(newJoins: IJoin[], oldJoins: IJoin[]) {
    const lines: AbstractAstElement[] = [];

    const oldCombinedJoins = groupJoinsByTableAndFilter(oldJoins);
    const newCombinedJoins = groupJoinsByTableAndFilter(newJoins);

    for (let i = 0, n = newCombinedJoins.length; i < n; i++) {
        const newCombinedJoin = newCombinedJoins[i];
        const oldCombinedJoin = oldCombinedJoins[i];
        
        lines.push(new If({
            if: isNotDistinctFrom([
                newCombinedJoin.byColumn
            ]),
            then: newCombinedJoin.variables.map((newVarName, j) => 
                new AssignVariable({
                    variable: newVarName,
                    value: new HardCode({
                        sql: oldCombinedJoin.variables[j]
                    })
                })
            ),
            else: [
                new If({
                    if: Expression.and([
                        `new.${newCombinedJoin.byColumn} is not null`
                    ]),
                    then: assignCombinedJoinVariables(
                        newCombinedJoin,
                        "new"
                    )
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