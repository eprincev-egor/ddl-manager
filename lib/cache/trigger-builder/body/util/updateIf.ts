import {
    If,
    Expression,
    Update
} from "../../../../ast";

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