"use strict";

const Syntax = require("./Syntax");
const CreateFunction = require("./CreateFunction");

class CommentOn extends Syntax {
    parse(coach) {
        coach.expectWord("comment");
        coach.skipSpace();
        coach.expectWord("on");
        coach.skipSpace();

        let hasObjectType = false;

        if ( coach.isWord("function") ) {
            coach.expectWord("function");
            coach.skipSpace();

            // {schema, name, args}
            this.function = CreateFunction.parseFunctionIdentify(coach);

            hasObjectType = true;
        }
        else if ( coach.isWord("trigger") ) {
            coach.expectWord("trigger");
            coach.skipSpace();

            this.trigger = {
                name: coach.readWord()
            };

            coach.skipSpace();
            coach.expectWord("on");
            coach.skipSpace();

            let {schema, name} = coach.parseSchemaName();
            this.trigger.schema = schema;
            this.trigger.table = name;

            hasObjectType = true;
        }

        if ( !hasObjectType ) {
            coach.throwError("expected function or trigger");
        }

        coach.skipSpace();
        coach.expectWord("is");
        coach.skipSpace();

        this.comment = coach.parsePgString();
    }
    
    is(coach) {
        return coach.isWord("comment");
    }
    
    clone() {
        let clone = new CommentOn();

        if ( this.function ) {
            clone.function = {
                schema: this.function.schema,
                name: this.function.name,
                args: this.function.args.slice()
            };
        }
        else {
            clone.trigger = {
                name: this.trigger.name,
                schema: this.trigger.schema,
                table: this.trigger.table
            };
        }

        clone.comment = this.comment.clone();

        return clone;
    }
    
    toString() {
        return CommentOn.comment2sql( this );
    }

    static comment2sql(comment) {
        let commentContent = comment.comment;
        // if commentContent is PgString
        if ( commentContent.content ) {
            commentContent = commentContent.content;
        }

        if ( comment.function ) {
            let {schema, name, args} = comment.function;
            return `comment on function ${schema}.${name}(${ args.join(", ") }) is $$${ commentContent }$$`;
        }
        else {
            let {name, schema, table} = comment.trigger;
            return `comment on trigger ${name} on ${schema}.${table} is $$${ commentContent }$$`;
        }
    }

    static comment2dropSql(comment) {
        if ( comment.function ) {
            let {schema, name, args} = comment.function;
            return `comment on function ${schema}.${name}(${ args.join(", ") }) is null`;
        }
        else {
            let {name, schema, table} = comment.trigger;
            return `comment on trigger ${name} on ${schema}.${table} is null`;
        }
    }

    toJSON() {
        let json = {};

        if ( this.function ) {
            json.function = {
                schema: this.function.schema,
                name: this.function.name,
                args: this.function.args.slice()
            };
        }
        else {
            json.trigger = {
                name: this.trigger.name,
                schema: this.trigger.schema,
                table: this.trigger.table
            };
        }

        json.comment = this.comment.content;

        return json;
    }
}

module.exports = CommentOn;
