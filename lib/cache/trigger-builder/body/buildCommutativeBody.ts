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
import { exitIf } from "./util/exitIf";
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

export interface IArrVar {
    name: string;
    type: string;
    triggerColumn: string;
}

export function buildCommutativeBody(
    hasMutableColumns: boolean,
    noChanges: Expression,
    oldJoins: IJoin[],
    newJoins: IJoin[],
    oldCase: ICase,
    newCase: ICase,
    deltaCase: IDeltaCase,
    insertedArrElements: IArrVar[],
    deletedArrElements: IArrVar[]
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
            ),
            ...insertedArrElements.map(insertedVar =>
                new Declare({
                    name: insertedVar.name,
                    type: insertedVar.type
                })
            ),
            ...deletedArrElements.map(deletedVar =>
                new Declare({
                    name: deletedVar.name,
                    type: deletedVar.type
                })
            ),
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
                        deltaCase,
                        insertedArrElements,
                        deletedArrElements
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
    deltaCase: IDeltaCase,
    insertedArrElements: IArrVar[],
    deletedArrElements: IArrVar[]
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

        ...assignArrVars(
            insertedArrElements,
            deletedArrElements
        ),

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
        
        const newByColumn = replaceTableToVariableOrRow(
            newCombinedJoin.byColumn,
            newJoins,
            "new"
        );
        const oldByColumn = replaceTableToVariableOrRow(
            oldCombinedJoin.byColumn,
            oldJoins,
            "old"
        );

        lines.push(new If({
            if: Expression.and([
                newByColumn + " is not distinct from " + oldByColumn
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
                        `${newByColumn} is not null`
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

function assignArrVars(
    insertedArrElements: IArrVar[],
    deletedArrElements: IArrVar[]
) {
    const output: AbstractAstElement[] = [];

    for (const insertedVar of insertedArrElements) {
        const assign = new AssignVariable({
            variable: insertedVar.name,
            value: new HardCode({
                sql: `cm_get_inserted_elements(old.${insertedVar.triggerColumn}, new.${insertedVar.triggerColumn})`
            })
        });
        output.push(assign);
    }

    for (const deletedVar of deletedArrElements) {
        const assign = new AssignVariable({
            variable: deletedVar.name,
            value: new HardCode({
                sql: `cm_get_deleted_elements(old.${deletedVar.triggerColumn}, new.${deletedVar.triggerColumn})`
            })
        });
        output.push(assign);
    }

    if ( output.length ) {
        output.push(new BlankLine());
    }

    return output;
}