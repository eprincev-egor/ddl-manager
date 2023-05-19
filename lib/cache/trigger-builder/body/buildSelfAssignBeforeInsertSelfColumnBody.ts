import {
    Body,
    HardCode,
    BlankLine,
    AssignVariable,
    SelectColumn
} from "../../../ast";

export function buildSelfAssignBeforeInsertSelfColumnBody(
    selectNewValue: SelectColumn
) {
    const body = new Body({
        statements: [
            new AssignVariable({
                variable: `new.${selectNewValue.name}`,
                value: selectNewValue.expression
            }),

            new BlankLine(),
            new HardCode({
                sql: `return new;`
            })
        ]
    });

    return body;
}