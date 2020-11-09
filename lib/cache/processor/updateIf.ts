import {
    If,
    Expression
} from "../../ast";
import { Update } from "../../ast/Update";

export function updateIf(
    condition: Expression | undefined,
    update: Update
) {
    if ( !condition ) {
        return update;
    }

    return new If({
        if: condition,
        then: [
            update
        ]
    });
}