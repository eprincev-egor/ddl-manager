import { Spaces } from "../Spaces";
import { Expression } from "./Expression";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { flatMap } from "../../utils";

interface ICase {
    when: Expression;
    then: Expression;
}

export class CaseWhen extends AbstractExpressionElement {
    readonly cases: ICase[];
    readonly else?: Expression;

    constructor(params: {cases: ICase[], else?: Expression}) {
        super();
        this.cases = params.cases;
        this.else = params.else;
    }

    clone() {
        return new CaseWhen({
            cases: this.cases.map(someCase => ({
                when: someCase.when.clone(),
                then: someCase.then.clone()
            })),
            else: this.else ? this.else.clone() : undefined
        });
    }

    template(spaces: Spaces): string[] {
        return [
            spaces + "case",
            ...flatMap(this.cases, someCase =>
                this.printCase(someCase, spaces)
            ),
            ...this.printElse(spaces),
            spaces + "end"
        ];
    }

    private printCase(someCase: ICase, spaces: Spaces) {
        return [
            spaces.plusOneLevel() + "when",
            someCase.when.toSQL(
                spaces
                    .plusOneLevel()
                    .plusOneLevel()
            ),
            spaces.plusOneLevel() + "then",
            someCase.then.toSQL(
                spaces
                    .plusOneLevel()
                    .plusOneLevel()
            )
        ];
    }

    private printElse(spaces: Spaces) {
        if ( !this.else ) {
            return [];
        }

        return [
            spaces.plusOneLevel() + "else",
            ...this.else.template(
                spaces
                    .plusOneLevel()
                    .plusOneLevel()
            )
        ];
    }
}