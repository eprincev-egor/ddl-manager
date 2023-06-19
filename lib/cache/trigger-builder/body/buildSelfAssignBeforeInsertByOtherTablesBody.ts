import {
    Body,
    HardCode,
    BlankLine,
    Expression,
    Declare,
    AssignVariable,
    Select
} from "../../../ast";
import { exitIf } from "./util/exitIf";

export function buildSelfAssignBeforeInsertByOtherTablesBody(
    selectNewValues: Select,
    notMatchedFilterOnInsert?: Expression,
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
            ...exitIf({
                if: notMatchedFilterOnInsert,
                blanksBefore: [new BlankLine()]
            }),
            new BlankLine(),
            new BlankLine(),

            selectNewValues.clone({
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