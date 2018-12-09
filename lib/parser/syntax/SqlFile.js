"use strict";

const Syntax = require("./Syntax");
const CreateFunction = require("./CreateFunction");
const CreateTrigger = require("./CreateTrigger");

class SqlFile extends Syntax {
    parse(coach) {
        coach.skipSpace();
        this.function = coach.parseCreateFunction();

        // comment on function
        let comment = this.parseComment( coach );
        if ( comment ) {
            if ( !comment.function ) {
                coach.throwError("comment after function, must be: comment on function");
            }

            let {schema, name, args} = comment.function;
            let identify = `${schema}.${name}(${ args.join(", ") })`;
            let shouldBeIdentify = CreateFunction.function2identifySql( this.function );

            if ( identify != shouldBeIdentify ) {
                coach.throwError("comment after function has wrong identify: " + identify);
            }
        }
        

        this.parseTriggers( coach );
    }

    parseComment(coach) {
        let i = coach.i;
        coach.skipSpace();

        if ( coach.is(";") ) {
            coach.expect(";");
            coach.skipSpace();

            if ( coach.isCommentOn() ) {
                let comment = coach.parseCommentOn();

                if ( !this.comments ) {
                    this.comments = [];
                }

                this.comments.push(comment);

                return comment;
            }
            else {
                coach.i = i;
            }
        }
    }

    parseTriggers(coach) {
        coach.skipSpace();

        if ( !coach.is(";") ) {
            return;
        }

        coach.expect(";");
        coach.skipSpace();

        if ( !coach.isCreateTrigger() ) {
            return;
        }

        let trigger = coach.parseCreateTrigger();

        // validate function name and trigger procedure
        if ( 
            this.function.schema != trigger.procedure.schema ||
            this.function.name != trigger.procedure.name
        ) {
            throw new Error(`wrong procedure name ${
                trigger.procedure.schema
            }.${
                trigger.procedure.name
            }`);
        }

        // validate function returns type
        if ( this.function.returns.type !== "trigger" ) {
            throw new Error(`wrong returns type ${ this.function.returns.type }`);
        }
        
        if ( !this.triggers ) {
            this.triggers = [];
        }
        this.triggers.push(trigger);

        // comment on trigger
        let comment = this.parseComment( coach );
        if ( comment ) {
            if ( !comment.trigger ) {
                coach.throwError("comment after trigger, must be: comment on trigger");
            }

            let {schema, table, name} = comment.trigger;
            let identify = `${name} on ${schema}.${table}`;
            let shouldBeIdentify = CreateTrigger.trigger2identifySql( trigger );

            if ( identify != shouldBeIdentify ) {
                coach.throwError("comment after trigger has wrong identify: " + identify);
            }
        }

        this.parseTriggers( coach );
    }
    
    is(coach) {
        let i = coach.i;

        coach.skipSpace();
        let isSqlFile = coach.isCreateFunction();

        coach.i = i;

        return isSqlFile;
    }
    
    clone() {
        let clone = new SqlFile();
        
        clone.function = this.function.clone();

        if ( this.triggers ) {
            clone.triggers = this.triggers.map(trigger =>
                trigger.clone()
            );
        }

        if ( this.comments ) {
            clone.comments = this.comments.map(comment =>
                comment.clone()
            );
        }
        
        return clone;
    }
    
    toString() {
        let out = this.function.toString();

        if ( this.comments ) {
            let funcComment = this.comments.find(comment =>
                !!comment.function
            );

            if ( funcComment ) {
                out += ";";
                out += funcComment.toString();
            }
        }

        if ( this.triggers ) {
            this.triggers.forEach(trigger => {
                out += ";";
                out += trigger.toString();

                if ( this.comments ) {
                    let triggerComment = this.comments.find(comment =>
                        comment.trigger &&
                        comment.trigger.schema == trigger.table.schema &&
                        comment.trigger.table == trigger.table.name &&
                        comment.trigger.name == trigger.name
                    );

                    if ( triggerComment ) {
                        out += ";";
                        out += triggerComment.toString();
                    }
                }
            });
        }

        return out;
    }

    toJSON() {
        let json = {
            function: this.function.toJSON()
        };

        if ( this.triggers ) {
            json.triggers = this.triggers.map(trigger => 
                trigger.toJSON()
            );
        }

        if ( this.comments ) {
            json.comments = this.comments.map(comment =>
                comment.toJSON()
            );
        }

        return json;
    }
}

module.exports = SqlFile;
