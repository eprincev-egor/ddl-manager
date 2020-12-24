import {
    Body,
    If,
    HardCode,
    BlankLine,
    Expression,
    Declare,
    AssignVariable,
    SimpleSelect,
    AbstractAstElement,
    ColumnReference
} from "../../../ast";
import { updateIf } from "./util/updateIf";
import { Update } from "../../../ast/Update";
import { TableReference } from "../../../database/schema/TableReference";

export interface IVariable {
    name: string;
    type: string;
}

export interface IJoin {
    variable: IVariable;
    table: {
        ref: TableReference;
        column: ColumnReference;
    };
    on: {
        column: ColumnReference;
    }
}

export interface ICase {
    hasReferenceWithoutJoins?: Expression,
    needUpdate?: Expression;
    update: Update;
}

export interface IDeltaCase extends ICase {
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
                    ...updateIf(
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

        ...updateIf(
            oldCaseCondition,
            oldUpdate
        ),

        new BlankLine(),

        ...updateIf(
            newCaseCondition,
            newUpdate
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
                    replaceTableToVariableOrRow(
                        combinedJoin.byColumn,
                        joins,
                        row
                    ) + " is not null"
                ]),
                then: assignCombinedJoinVariables(
                    combinedJoin,
                    joins,
                    row
                )
            })
        );

        lines.push( new BlankLine() );
    }

    return lines;
}

function replaceTableToVariableOrRow(
    columnRef: ColumnReference,
    joins: IJoin[],
    row: "new" | "old"
) {
    const sourceJoin = joins.find(join =>
        join.table.ref.equal(columnRef.tableReference) &&
        join.table.column.name === columnRef.name
    );
    if ( sourceJoin ) {
        return sourceJoin.variable.name;
    }

    return `${row}.${columnRef.name}`
}

function assignCombinedJoinVariables(
    combinedJoin: ICombinedJoin,
    joins: IJoin[],
    row: "new" | "old"
) {
    if ( combinedJoin.variables.length === 1 ) {
        return [
            new AssignVariable({
                variable: combinedJoin.variables[0],
                value: new SimpleSelect({
                    columns: combinedJoin.joinedColumns,
                    from: combinedJoin.joinedTable.table,
                    where: replaceTableToVariableOrRow(
                        combinedJoin.byColumn,
                        joins,
                        row
                    )
                })
            })
        ]
    }
    
    return [
        new SimpleSelect({
            columns: combinedJoin.joinedColumns,
            into: combinedJoin.variables,
            from: combinedJoin.joinedTable.table,
            where: replaceTableToVariableOrRow(
                combinedJoin.byColumn,
                joins,
                row
            )
        })
    ];
}

interface ICombinedJoin {
    variables: string[];
    joinedColumns: string[];
    joinedTable: TableReference;
    byColumn: ColumnReference;
}
function groupJoinsByTableAndFilter(joins: IJoin[]) {
    const combinedJoinByKey: {[key: string]: ICombinedJoin} = {};

    for (const join of joins) {
        const key = join.on.column.toString();

        const defaultCombinedJoin: ICombinedJoin = {
            variables: [],
            joinedColumns: [],
            joinedTable: join.table.ref,
            byColumn: join.on.column
        };
        const combinedJoin: ICombinedJoin = combinedJoinByKey[ key ] || defaultCombinedJoin;

        combinedJoin.variables.push(
            join.variable.name
        );
        combinedJoin.joinedColumns.push(
            join.table.column.name
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
                newCombinedJoin.byColumn.name
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
                        `new.${newCombinedJoin.byColumn.name} is not null`
                    ]),
                    then: assignCombinedJoinVariables(
                        newCombinedJoin,
                        newJoins,
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