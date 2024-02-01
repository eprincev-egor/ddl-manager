import { AbstractNode, Cursor, Name, PgType, SchemaName, StringLiteral, TemplateElement } from "psql-lang";
import { CacheIndex } from "./CacheIndex";
import { CacheWithoutInsertOn } from "./CacheWithoutInsertOn";
import { CacheWithoutTriggersOn } from "./CacheWithoutTriggersOn";

export type CommentSyntaxRow = {
    comment: StringLiteral;
};

export class CommentSyntax extends AbstractNode<CommentSyntaxRow> {

    static entry(cursor: Cursor): boolean {
        return cursor.beforeWord("comment");
    }

    static parse(cursor: Cursor): CommentSyntaxRow {
        cursor.readPhrase("comment", "on");
        
        if ( cursor.beforeWord("function") ) {
            cursor.readWord("function");
            cursor.parse(SchemaName);
            cursor.skipSpaces();

            if ( cursor.beforeValue("(") ) {
                cursor.readValue("(");
                cursor.skipSpaces();

                cursor.parseChainOf(PgType, ",");

                cursor.skipSpaces();
                cursor.readValue(")");
                cursor.skipSpaces();
            }
        }
        else {
            cursor.readWord("trigger");
            cursor.parse(Name);
            cursor.skipSpaces();
            
            cursor.readWord("on");
            cursor.parse(SchemaName);
            cursor.skipSpaces();
        }

        cursor.readWord("is");
        const comment = cursor.parse(StringLiteral);

        return {comment};
    }

    template(): TemplateElement[] {
        return [];
    }
}