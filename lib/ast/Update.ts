import { AbstractAstElement } from "./AbstractAstElement";
import { SetItem } from "./SetItem";
import { SetSelectItem } from "./SetSelectItem";
import { Spaces } from "./Spaces";
import { With } from "./With";

interface UpdateRow {
    with?: With;
    table: string;
    set: (SetItem | SetSelectItem)[];
    from?: string[];
    where?: AbstractAstElement;
}

export class Update extends AbstractAstElement {

    readonly with?: With;
    readonly table!: string;
    readonly set!: (SetItem | SetSelectItem)[];
    readonly from?: string[];
    readonly where?: AbstractAstElement;

    constructor(row: UpdateRow) {
        super();
        Object.assign(this, row);
    }

    template(spaces: Spaces) {
        const lines = [
            spaces + `update ${ this.table } set`,
            this.printSetItems(spaces)
        ];
        
        if ( this.with ) {
            lines.unshift(
                this.with.toSQL(spaces)
            );
        }

        if ( this.from ) {
            lines.push(spaces + `from ${ this.from.join(", ") }`);
        }

        if ( this.where ) {
            lines.push(spaces + "where");
            lines.push( this.where.toSQL( spaces.plusOneLevel() ) );
        }

        lines[ lines.length -1 ] = lines[ lines.length -1 ] + ";";

        return lines;
    }

    private printSetItems(spaces: Spaces) {
        const sql = this.set
            .map(setItem =>
                setItem.toSQL( 
                    spaces.plusOneLevel()
                )
            )
            .join(",\n");
        return sql;
    }
}