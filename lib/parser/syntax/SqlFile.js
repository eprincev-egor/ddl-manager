"use strict";

const Syntax = require("./Syntax");
const CreateFunction = require("./CreateFunction");
const CreateTrigger = require("./CreateTrigger");

class SqlFile extends Syntax {
    parse(coach) {
        coach.skipSpace();

        this.functions = [];
        this.parseFunctions( coach );

        this.parseTriggers( coach );
    }

    parseFunctions(coach) {
        let func = coach.parseCreateFunction();
        
        // check duplicate
        let isDuplicate = this.functions.some(prevFunc =>
            CreateFunction.function2identifySql( prevFunc )
            == 
            CreateFunction.function2identifySql( func )
        );

        if ( isDuplicate ) {
            coach.throwError("duplicated function: " + CreateFunction.function2identifySql( func ));
        }

        // two function inside file, can be with only same name and schema
        let isWrongName = this.functions.some(prevFunc =>
            prevFunc.name != func.name ||
            prevFunc.schema != func.schema
        );

        if ( isWrongName ) {
            coach.throwError("two function inside file, can be with only same name and schema");
        }

        // save func
        this.functions.push(
            func
        );

        
        // comment on function
        let comment = this.parseComment( coach );
        if ( comment ) {
            if ( !comment.function ) {
                coach.throwError("comment after function, must be: comment on function");
            }

            let {schema, name, args} = comment.function;
            let identify = `${schema}.${name}(${ args.join(", ") })`;
            let shouldBeIdentify = CreateFunction.function2identifySql( func );

            if ( identify != shouldBeIdentify ) {
                coach.throwError("comment after function has wrong identify: " + identify);
            }
        }
        
        let i = coach.i;
        coach.skipSpace();

        if ( coach.is(";") ) {
            coach.expect(";");
            coach.skipSpace();

            if ( coach.isCreateFunction() ) {
                this.parseFunctions( coach );
            }
            else {
                coach.i = i;
            }
        }
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

        let firstFunc = this.functions[0];
        if ( !firstFunc ) {
            coach.throwError("trigger inside file can be only with function");
        }

        let trigger = coach.parseCreateTrigger();

        // validate function name and trigger procedure
        if ( 
            firstFunc.schema != trigger.procedure.schema ||
            firstFunc.name != trigger.procedure.name
        ) {
            throw new Error(`wrong procedure name ${
                trigger.procedure.schema
            }.${
                trigger.procedure.name
            }`);
        }

        // validate function returns type
        if ( firstFunc.returns.type !== "trigger" ) {
            throw new Error(`wrong returns type ${ firstFunc.returns.type }`);
        }
        
        if ( !this.triggers ) {
            this.triggers = [];
        }

        // check duplicate
        let isDuplicate = this.triggers.some(prevTrigger =>
            CreateTrigger.trigger2identifySql( prevTrigger )
            == 
            CreateTrigger.trigger2identifySql( trigger )
        );

        if ( isDuplicate ) {
            coach.throwError("duplicated trigger: " + CreateTrigger.trigger2identifySql( trigger ));
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
        
        clone.functions = this.functions.map(func =>
            func.clone()
        );

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
        let out = "";

        this.functions.forEach((func, i) => {
            if ( i > 0 ) {
                out += ";\n";
            }

            out += func.toString();

            if ( this.comments ) {
                let identifyJson = CreateFunction.function2identifyJson( func );
                identifyJson = JSON.stringify( identifyJson );

                let funcComment = this.comments.find(comment =>
                    comment.function &&
                    JSON.stringify(comment.function) == identifyJson
                );
    
                if ( funcComment ) {
                    out += ";\n";
                    out += funcComment.toString();
                }
            }
        });

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
            functions: this.functions.map(func => 
                func.toJSON()
            )
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
