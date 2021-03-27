import {
    Body,
    If,
    HardCode,
    BlankLine,
    Expression,
    Declare,
    ColumnReference,
    AssignVariable,
    AbstractAstElement,
    SimpleSelect
} from "../../../ast";
import { doIf } from "./util/doIf";
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
    hasReference?: Expression,
    needUpdate?: Expression;
    update: Update;
}

export interface IUpdateCase {
    deleted: {
        needUpdate: Expression;
        update: Update;
    };
    notChanged?: {
        needUpdate: Expression;
        update: Update;
    };
    inserted: {
        needUpdate: Expression;
        update: Update;
    };
}

export interface IArrVar {
    name: string;
    type: string;
    triggerColumn: string;
}

export interface ArrayCommutativeAst {
    needInsertCase: boolean;
    hasMutableColumns: boolean;
    noChanges: Expression;
    needMatching: boolean;
    matchedOld: AbstractAstElement;
    matchedNew: AbstractAstElement;
    insertCase: ICase;
    deleteCase: ICase;
    updateCase: IUpdateCase;
    insertedArrElements: IArrVar[];
    notChangedArrElements: IArrVar[];
    deletedArrElements: IArrVar[];
    oldJoins: IJoin[];
    newJoins: IJoin[];
}

export function buildArrayCommutativeBody(ast: ArrayCommutativeAst) {
    const body = new Body({
        declares: [
            ...ast.oldJoins.map(join =>
                new Declare({
                    name: join.variable.name,
                    type: join.variable.type
                })
            ),
            ...ast.newJoins.map(join => 
                new Declare({
                    name: join.variable.name,
                    type: join.variable.type
                })
            ),
            ...(
                ast.needMatching ? [
                    new Declare({
                        name: "matched_old",
                        type: "boolean"
                    }),
                    new Declare({
                        name: "matched_new",
                        type: "boolean"
                    }),
                ] : []
            ),
            ...ast.insertedArrElements.map(insertedVar =>
                new Declare({
                    name: insertedVar.name,
                    type: insertedVar.type
                })
            ),
            ...ast.notChangedArrElements.map(insertedVar =>
                new Declare({
                    name: insertedVar.name,
                    type: insertedVar.type
                })
            ),
            ...ast.deletedArrElements.map(deletedVar =>
                new Declare({
                    name: deletedVar.name,
                    type: deletedVar.type
                })
            ),
        ],
        statements: [
            ...buildInsertOrDeleteCase(
                "DELETE",
                ast.deleteCase,
                ast.oldJoins,
                "old"
            ),

            ...(ast.hasMutableColumns ? [
                new If({
                    if: new HardCode({sql: "TG_OP = 'UPDATE'"}),
                    then: buildUpdateCaseBody(ast)
                })
            ] : []),

            ...(ast.needInsertCase ? buildInsertOrDeleteCase(
                "INSERT",
                ast.insertCase,
                ast.newJoins,
                "new"
            ): [])
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

                ...doIf(simpleCase.hasReference, [
                    ...assignVariables(joins, returnRow),
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

function buildUpdateCaseBody(ast: ArrayCommutativeAst) {
    const {inserted, notChanged, deleted} = ast.updateCase;

    return [
        new If({
            if: ast.noChanges,
            then: [
                new HardCode({sql: "return new;"})
            ]
        }),
        new BlankLine(),

        ...(ast.needMatching ? [

            new AssignVariable({
                variable: "matched_old",
                value: ast.matchedOld
            }),
            new AssignVariable({
                variable: "matched_new",
                value: ast.matchedNew
            }),
            new BlankLine(),
    
            new If({
                if: Expression.and([
                    "not matched_old",
                    "not matched_new"
                ]),
                then: [
                    new HardCode({sql: "return new;"})
                ]
            }),
            new BlankLine(),
            new If({
                if: Expression.and([
                    "matched_old",
                    "not matched_new"
                ]),
                then: [
                    ...ast.insertedArrElements.map(insertedVar =>
                        new AssignVariable({
                            variable: insertedVar.name,
                            value: Expression.unknown("null")
                        })
                    ),
                    ...ast.notChangedArrElements.map(notChangedVar =>
                        new AssignVariable({
                            variable: notChangedVar.name,
                            value: Expression.unknown("null")
                        })
                    ),
                    ...ast.deletedArrElements.map(deletedVar =>
                        new AssignVariable({
                            variable: deletedVar.name,
                            value: Expression.unknown(`old.${ deletedVar.triggerColumn }`)
                        })
                    )
                ]
            }),
            new BlankLine(),
            new If({
                if: Expression.and([
                    "not matched_old",
                    "matched_new"
                ]),
                then: [
                    ...ast.insertedArrElements.map(insertedVar =>
                        new AssignVariable({
                            variable: insertedVar.name,
                            value: Expression.unknown(`new.${ insertedVar.triggerColumn }`)
                        })
                    ),
                    ...ast.notChangedArrElements.map(notChangedVar =>
                        new AssignVariable({
                            variable: notChangedVar.name,
                            value: Expression.unknown("null")
                        })
                    ),
                    ...ast.deletedArrElements.map(deletedVar =>
                        new AssignVariable({
                            variable: deletedVar.name,
                            value: Expression.unknown("null")
                        })
                    )
                ]
            }),
    
            new BlankLine(),
            new If({
                if: Expression.and([
                    "matched_old",
                    "matched_new"
                ]),
                then: [
                    ...ast.insertedArrElements.map(insertedVar =>
                        new AssignVariable({
                            variable: insertedVar.name,
                            value: Expression.unknown(
                                `cm_get_inserted_elements(old.${insertedVar.triggerColumn}, new.${insertedVar.triggerColumn})`
                            )
                        })
                    ),
                    ...ast.notChangedArrElements.map(notChangedVar =>
                        new AssignVariable({
                            variable: notChangedVar.name,
                            value: Expression.unknown(
                                `cm_get_not_changed_elements(old.${notChangedVar.triggerColumn}, new.${notChangedVar.triggerColumn})`
                            )
                        })
                    ),
                    ...ast.deletedArrElements.map(deletedVar =>
                        new AssignVariable({
                            variable: deletedVar.name,
                            value: Expression.unknown(
                                `cm_get_deleted_elements(old.${deletedVar.triggerColumn}, new.${deletedVar.triggerColumn})`
                            )
                        })
                    )
                ]
            }),
        ] : [
            ...ast.insertedArrElements.map(insertedVar =>
                new AssignVariable({
                    variable: insertedVar.name,
                    value: Expression.unknown(
                        `cm_get_inserted_elements(old.${insertedVar.triggerColumn}, new.${insertedVar.triggerColumn})`
                    )
                })
            ),
            ...ast.notChangedArrElements.map(notChangedVar =>
                new AssignVariable({
                    variable: notChangedVar.name,
                    value: Expression.unknown(
                        `cm_get_not_changed_elements(old.${notChangedVar.triggerColumn}, new.${notChangedVar.triggerColumn})`
                    )
                })
            ),
            ...ast.deletedArrElements.map(deletedVar =>
                new AssignVariable({
                    variable: deletedVar.name,
                    value: Expression.unknown(
                        `cm_get_deleted_elements(old.${deletedVar.triggerColumn}, new.${deletedVar.triggerColumn})`
                    )
                })
            )
        ]),

        new BlankLine(),

        ...assignVariables(ast.oldJoins, "old"),
        ...reassignVariables(
            ast.newJoins,
            ast.oldJoins
        ),

        ...doIf(
            notChanged && notChanged.needUpdate,
            notChanged ? [notChanged.update] : []
        ),

        new BlankLine(),

        ...doIf(
            deleted.needUpdate,
            [deleted.update]
        ),

        new BlankLine(),

        ...doIf(
            inserted.needUpdate,
            [inserted.update]
        ),
        new BlankLine(),
        new HardCode({sql: "return new;"})
    ];
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
