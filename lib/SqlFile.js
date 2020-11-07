"use strict";

const {Syntax, Types} = require("lang-coach");
const {
    CreateFunction,
    CreateTrigger,
    CommentOn
} = require("grapeql-lang");
const {
    function2identifySql,
    trigger2identifySql,
    function2identifyJson
} = require("./utils");

class SqlFile extends Syntax {
    structure() {
        return {
            functions: Types.Array({
                element: CreateFunction
            }),
            triggers: Types.Array({
                element: CreateTrigger
            }),
            comments: Types.Array({
                element: CommentOn
            })
        };
    }

    parse(coach, data) {
        coach.skipSpace();

        data.functions = [];
        this.parseFunctions( coach, data );

        this.parseTriggers( coach, data );
    }

    parseFunctions(coach, data) {
        let func = coach.parse(CreateFunction);
        
        // check duplicate
        let isDuplicate = data.functions.some(prevFunc =>
            function2identifySql( prevFunc )
            == 
            function2identifySql( func )
        );

        if ( isDuplicate ) {
            coach.throwError("duplicated function: " + function2identifySql( func ));
        }

        // two function inside file, can be with only same name and schema
        let isWrongName = data.functions.some(prevFunc =>
            prevFunc.row.name != func.row.name ||
            prevFunc.row.schema != func.row.schema
        );

        if ( isWrongName ) {
            coach.throwError("two function inside file, can be with only same name and schema");
        }

        // save func
        data.functions.push(
            func
        );

        
        // comment on function
        let comment = this.parseComment( coach, data );
        if ( comment ) {
            if ( !comment.row.function ) {
                coach.throwError("comment after function, must be: comment on function");
            }

            let {schema, name, args} = comment.row.function;
            let identify = `${schema}.${name}(${ args.join(", ") })`;
            let shouldBeIdentify = function2identifySql( func );

            if ( identify != shouldBeIdentify ) {
                coach.throwError("comment after function has wrong identify: " + identify);
            }
        }

        coach.skipSpace();
        coach.read(/[\s;]+/);

        if ( coach.is(CreateFunction) ) {
            this.parseFunctions( coach, data );
        }
    }

    parseComment(coach, data) {
        coach.skipSpace();
        coach.read(/[\s;]+/);

        if ( coach.is(CommentOn) ) {
            let comment = coach.parse(CommentOn);

            if ( !data.comments ) {
                data.comments = [];
            }

            data.comments.push(comment);

            return comment;
        }
    }

    parseTriggers(coach, data) {
        coach.skipSpace();

        // skip spaces and some ;
        coach.read(/[\s;]+/);

        if ( !coach.is(CreateTrigger) ) {
            return;
        }

        let firstFunc = data.functions[0];
        if ( !firstFunc ) {
            coach.throwError("trigger inside file can be only with function");
        }

        let trigger = coach.parse(CreateTrigger);

        // validate function name and trigger procedure
        if ( 
            firstFunc.row.schema != trigger.row.procedure.row.schema ||
            firstFunc.row.name != trigger.row.procedure.row.name
        ) {
            throw new Error(`wrong procedure name ${
                trigger.row.procedure.row.schema
            }.${
                trigger.row.procedure.row.name
            }`);
        }

        // validate function returns type
        let hasTriggerFunc = data.functions.some(func =>
            func.row.returns.row.type == "trigger"
        );
        if ( !hasTriggerFunc ) {
            throw new Error("file must contain function with returns type trigger");
        }
        
        if ( !data.triggers ) {
            data.triggers = [];
        }

        // check duplicate
        let isDuplicate = data.triggers.some(prevTrigger =>
            trigger2identifySql( prevTrigger )
            == 
            trigger2identifySql( trigger )
        );

        if ( isDuplicate ) {
            coach.throwError("duplicated trigger: " + trigger2identifySql( trigger ));
        }

        data.triggers.push(trigger);

        // comment on trigger
        let comment = this.parseComment( coach, data );
        if ( comment ) {
            if ( !comment.row.trigger ) {
                coach.throwError("comment after trigger, must be: comment on trigger");
            }

            let {schema, table, name} = comment.row.trigger.row;
            let identify = `${name} on ${schema}.${table}`;
            let shouldBeIdentify = trigger2identifySql( trigger );

            if ( identify != shouldBeIdentify ) {
                coach.throwError("comment after trigger has wrong identify: " + identify);
            }
        }

        this.parseTriggers( coach, data );
    }
    
    is(coach) {
        let i = coach.i;

        coach.skipSpace();
        let isSqlFile = coach.is(CreateFunction);

        coach.i = i;

        return isSqlFile;
    }
    
    toString() {
        let out = "";

        this.row.functions.forEach((func, i) => {
            if ( i > 0 ) {
                out += ";\n";
            }

            out += func.toString();

            if ( this.row.comments ) {
                let identifyJson = function2identifyJson( func );
                identifyJson = JSON.stringify( identifyJson );

                let funcComment = this.row.comments.find(comment =>
                    comment.row.function &&
                    JSON.stringify(comment.row.function) == identifyJson
                );
    
                if ( funcComment ) {
                    out += ";\n";
                    out += funcComment.toString();
                }
            }
        });

        if ( this.row.triggers ) {
            this.row.triggers.forEach(trigger => {
                out += ";";
                out += trigger.toString();

                if ( this.row.comments ) {
                    let triggerComment = this.row.comments.find(comment =>
                        comment.row.trigger &&
                        comment.row.trigger.row.schema == trigger.row.table.row.schema &&
                        comment.row.trigger.row.table == trigger.row.table.row.name &&
                        comment.row.trigger.row.name == trigger.row.name
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

}

module.exports = {SqlFile};
