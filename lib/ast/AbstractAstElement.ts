import { Spaces } from "./Spaces";

export abstract class AbstractAstElement {

    abstract template(spaces: Spaces): string[];
    
    toSQL(parentSpaces = Spaces.empty()): string {
        const lines = this.template(parentSpaces);
        const sql = lines.join("\n");
        return sql;
    }

    toString() {
        return this.toSQL();
    }
}