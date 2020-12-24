import {
    If,
    Expression,
    Update
} from "../../../../ast";

export function updateIf(
    condition: Expression | undefined,
    update: Update | undefined
) {
    if ( !update ) {
        return [];
    }

    if ( !condition ) {
        return [update];
    }

    return [new If({
        if: condition,
        then: [
            update
        ]
    })];
}