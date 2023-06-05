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
import { exitIf } from "./util/exitIf";

export function buildSelfUpdateByOtherTablesBody(
    noChanges: Expression,
    selectNewValues: Select,
    notMatchedFilterOnUpdate?: Expression,
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
                if: noChanges,
                then: [
                    new HardCode({
                        sql: `return new;`
                    })
                ]
            }),
            ...exitIf({
                if: notMatchedFilterOnUpdate,
                blanksBefore: [new BlankLine()]
            }),
            new BlankLine(),
            new BlankLine(),
            
            selectNewValues.cloneWith({
                intoRow: "new_totals"
            }),
            new BlankLine(),
            new BlankLine(),
            ...selectNewValues.columns.map(column => 
                new AssignVariable({
                    variable: `new.${column.name}`,
                    value: new HardCode({sql: `new_totals.${column.name}`})
                })
            ),
            
            new BlankLine(),
            new BlankLine(),
            new HardCode({
                sql: `return new;`
            })
        ]
    });

    return body;
}