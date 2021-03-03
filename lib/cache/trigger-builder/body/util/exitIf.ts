import {
    If,
    HardCode,
    BlankLine,
    Expression
} from "../../../../ast";

export function exitIf(params: {
    if?: Expression;
    return?: "new" | "old";
    blanksBefore?: BlankLine[];
    blanksAfter?: BlankLine[];
}) {
    if ( !params.if ) {
        return [];
    }

    return [
        ...(params.blanksBefore || []),
        new If({
            if: params.if,
            then: [
                new HardCode({sql: `return ${params.return || "new"};`})
            ]
        }),
        ...(params.blanksAfter || [])
    ];
}
