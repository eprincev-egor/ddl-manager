import {
    Body,
    If,
    HardCode,
    BlankLine,
    Expression,
    Declare,
    Select,
    AssignVariable
} from "../../../ast";

export function buildSelfUpdateBySelfRowBody(
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

            ...(noChanges.isEmpty() ? [] : [
                new If({
                    if: noChanges,
                    then: [
                        new HardCode({
                            sql: `return new;`
                        })
                    ]
                }),
                new BlankLine(),
                new BlankLine(),
            ]),

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

                    ...selectNewValues.columns.map(column => 
                        new AssignVariable({
                            variable: `new.${column.name}`,
                            value: new HardCode({sql: `new_totals.${column.name}`})
                        })
                    ),

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