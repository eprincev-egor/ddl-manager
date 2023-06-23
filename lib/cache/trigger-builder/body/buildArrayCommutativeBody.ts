import {
    Body,
    If,
    HardCode,
    BlankLine,
    Expression,
    Declare,
    AssignVariable,
    AbstractAstElement
} from "../../../ast";
import { doIf } from "./util/doIf";
import { Update } from "../../../ast/Update";
import { IArrVar } from "../../processor/buildArrVars";
import { IJoin } from "../../processor/buildJoinVariables";

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
}

export function buildArrayCommutativeBody(ast: ArrayCommutativeAst) {
    const body = new Body({
        declares: [
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

                ...doIf(simpleCase.hasReference, [
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
