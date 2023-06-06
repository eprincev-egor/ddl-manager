import {
    Body,
    HardCode,
    BlankLine,
    AssignVariable,
    SelectColumn
} from "../../../ast";

export function buildSelfAssignBeforeInsertSelfColumnBody(
    selectColumns: SelectColumn[]
) {
    const body = new Body({
        statements: [
            ...selectColumns.map(column => 
                new AssignVariable({
                    variable: `new.${column.name}`,
                    value: column.expression
                })
            ),

            new BlankLine(),
            new HardCode({
                sql: `return new;`
            })
        ]
    });

    return body;
}