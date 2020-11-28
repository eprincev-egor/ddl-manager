import {
    Body,
    If,
    HardCode,
    BlankLine,
    Update,
    TableReference,
    Expression,
    Declare,
    Select,
    SetItem
} from "../../../ast";

export function buildSelfUpdateBySelfRowBody(
    updateTable: TableReference,
    noChanges: Expression,
    selectNewValues: Select
) {
    const body = new Body({
        declares: [
            new Declare({
                name: "new_totals",
                type: "record"
            })
        ],
        statements: [
            new BlankLine(),
            new If({
                if: new HardCode({
                    sql: `TG_OP = 'UPDATE'`
                }),
                then: [
                    new If({
                        if: noChanges,
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


            selectNewValues.cloneWith({
                intoRow: "new_totals"
            }),

            new BlankLine(),

            new If({
                if: Expression.or(
                    selectNewValues.columns.map(updateColumn => 
                        `new_totals.${updateColumn.name} is distinct from new.${updateColumn.name}`
                    )
                ),
                then: [
                    new BlankLine(),
                    
                    new Update({
                        table: updateTable.toString(),
                        set: selectNewValues.columns.map(updateColumn => 
                            new SetItem({
                                column: updateColumn.name,
                                value: new HardCode({
                                    sql: `new_totals.${updateColumn.name}`
                                })
                            })
                        ),
                        where: new HardCode({
                            sql: `${updateTable.getIdentifier()}.id = new.id`
                        })
                    }),

                    new BlankLine()
                ]
            }),
            
            new BlankLine(),
            new HardCode({
                sql: `return new;`
            })
        ]
    });

    return body;
}