import {
    AbstractNode, Cursor, TemplateElement,
    CreateFunction, CreateTrigger
} from "psql-lang";
import { CacheSyntax } from "./CacheSyntax";
import { CacheLinter } from "./CacheLinter";
import { CommentSyntax } from "./CommentSyntax";

export interface FileSyntaxRow {
    functions: CreateFunction[];
    triggers: CreateTrigger[];
    caches: CacheSyntax[];
}

export class FileSyntax extends AbstractNode<FileSyntaxRow> {

    static entry(cursor: Cursor): boolean {
        return (
            cursor.before(CreateFunction) ||
            cursor.before(CreateTrigger) ||
            cursor.before(CacheSyntax) 
        );
    }

    static parse(cursor: Cursor): FileSyntaxRow {
        const output: FileSyntaxRow = {
            functions: [],
            triggers: [],
            caches: []
        };

        while ( !cursor.beforeEnd() ) {
            this.parseDelimiters(cursor);

            if ( cursor.before(CreateFunction) ) {
                const function_ = cursor.parse(CreateFunction);
                output.functions.push(function_);
            }
            else if ( cursor.before(CreateTrigger) ) {
                const trigger = cursor.parse(CreateTrigger);
                output.triggers.push(trigger);
            }
            else if ( cursor.before(CacheSyntax) ) {
                const cache = cursor.parse(CacheSyntax);
                CacheLinter.lint(cursor, cache);

                output.caches.push(cache);
            }
            else if ( cursor.before(CommentSyntax) ) {
                cursor.parse(CommentSyntax); // just ignore
            }
            else if ( !cursor.beforeEnd() ) {
                cursor.throwError("expected function or trigger or cache")
            }
        }

        return output;
    }

    private static parseDelimiters(cursor: Cursor) {
        cursor.skipSpaces();

        while ( cursor.beforeValue(";") ) {
            cursor.readValue(";");
            cursor.skipSpaces();
        }
    }

    template(): TemplateElement[] {
        return [
            ...this.row.functions,
            ...this.row.triggers,
            ...this.row.caches,
        ];
    }
}