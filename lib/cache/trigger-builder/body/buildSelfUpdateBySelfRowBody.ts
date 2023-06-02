import {
    Body,
    If,
    HardCode,
    BlankLine,
    Expression,
    Declare,
    AssignVariable,
    SelectColumn
} from "../../../ast";

export function buildSelfUpdateBySelfRowBody(
    noChanges: Expression,
    selectNewValues: SelectColumn[]
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

            ...selectNewValues.map(column => new AssignVariable({
                variable: `new.${column.name}`,
                value: column.expression
            })),

            new BlankLine(),
            new HardCode({
                sql: `return new;`
            })
        ]
    });

    return body;
}