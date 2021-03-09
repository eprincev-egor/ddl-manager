import {
    If,
    Expression,
    AbstractAstElement
} from "../../../../ast";

export function doIf(
    condition: Expression | undefined,
    doBlock: AbstractAstElement[]
) {
    if ( !doBlock.length ) {
        return [];
    }

    if ( !condition ) {
        return doBlock;
    }

    return [new If({
        if: condition,
        then: [
            ...doBlock
        ]
    })];
}