import {  } from "grapeql-lang";
import { AbstractAstElement } from "./AbstractAstElement";
import { Spaces } from "./Spaces";

interface IfRow {
    if: AbstractAstElement;
    then: AbstractAstElement[];
    else?: AbstractAstElement[];
}

export class If extends AbstractAstElement {

    readonly if!: AbstractAstElement;
    readonly then!: AbstractAstElement[];
    readonly else?: AbstractAstElement[];

    constructor(row: IfRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces): string[] {
        return [
            spaces + `if${ this.printCondition(spaces) }then`,
            ...this.then.map(child => 
                child.toSQL( spaces.plusOneLevel() )
            ),
            ...(this.else ?
                [
                    spaces + "else",
                    ...this.else.map(child =>
                        child.toSQL( spaces.plusOneLevel() )
                    )
                ] :
                []
            ),
            spaces + 'end if;'
        ];
    }

    private printCondition(spaces: Spaces) {
        const conditionSQL = this.if.toSQL( spaces.plusOneLevel() );

        if ( /\n/.test(conditionSQL) ) {
            return `\n${conditionSQL}\n${spaces}`;
        }

        return ` ${conditionSQL.trim()} `;
    }
}