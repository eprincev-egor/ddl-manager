import { Expression } from "../../../ast";

export function isNotDistinctFrom(columns: string[]) {
    const conditions = columns.map(column =>
        `new.${ column } is not distinct from old.${ column }`
    );
    return Expression.and(conditions);
}
