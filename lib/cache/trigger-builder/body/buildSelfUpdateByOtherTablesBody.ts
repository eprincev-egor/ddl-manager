import {
    Body,
    If,
    HardCode,
    BlankLine,
    Update,
    SetSelectItem,
    TableReference,
    Expression
} from "../../../ast";

export function buildSelfUpdateByOtherTablesBody(
    updateTable: TableReference,
    noReferenceChanges: Expression,
    hasReference: Expression,
    columnsToUpdate: string[],
    selectNewValues: string
) {
    const body = new Body({
        statements: [
            new If({
                if: new HardCode({
                    sql: `TG_OP = 'INSERT'`
                }),
                then: [
                    new If({
                        if: hasReference,
                        then: [
                            new HardCode({
                                sql: `return new;`
                            })
                        ]
                    })
                ]
            }),
            new BlankLine(),
            new If({
                if: new HardCode({
                    sql: `TG_OP = 'UPDATE'`
                }),
                then: [
                    new If({
                        if: noReferenceChanges,
                        then: [
                            new HardCode({
                                sql: `return new;`
                            })
                        ]
                    }),
                ]
            }),
            new BlankLine(),
            new BlankLine(),
            
            new Update({
                table: updateTable.toString(),
                set: [new SetSelectItem({
                    columns: columnsToUpdate,
                    select: selectNewValues
                })],

                where: new HardCode({
                    sql: `${updateTable.getIdentifier()}.id = new.id`
                })
            }),
            
            new BlankLine(),
            new BlankLine(),
            new HardCode({
                sql: `return new;`
            })
        ]
    });

    return body;
}