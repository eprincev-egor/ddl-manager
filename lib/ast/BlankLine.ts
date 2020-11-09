import { AbstractAstElement } from "./AbstractAstElement";

export class BlankLine extends AbstractAstElement {
    template() {
        return [
            ""
        ];
    }
}