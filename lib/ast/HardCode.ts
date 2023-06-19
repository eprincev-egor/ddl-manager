import { AbstractAstElement } from "./AbstractAstElement";
import { Spaces } from "./Spaces";

interface HardCodeRow {
    sql: string | string[];
}

export class HardCode extends AbstractAstElement {

    readonly sql!: string | string[];

    constructor(row: HardCodeRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces) {
        const lines: string[] = [];

        for (const line of this.prepareLines()) {
            if ( !line.trim() ) {
                lines.push("");
            }
            else {
                lines.push(spaces + line);
            }
        }
        
        return lines;
    }

    clone() {
        return new HardCode({
            sql: typeof this.sql === "string" ?
                this.sql : this.sql.slice()
        })
    }

    private prepareLines(): string[] {
        if ( typeof this.sql === "string" ) {
            return [this.sql];
        }
        return this.sql;
    }
}