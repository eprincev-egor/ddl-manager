"use strict";

const Syntax = require("./Syntax");

class CreateFunction extends Syntax {
    is(coach) {
        return coach.is(/^create(\s+or\s+replace)?\s+function/i);
    }

    parse(coach) {
        // create or replace function
        coach.expectWord("create");
        coach.skipSpace();
        

        if ( coach.isWord("or") ) {
            coach.expectWord("or");
            coach.skipSpace();

            coach.expectWord("replace");
            coach.skipSpace();
        }

        coach.expectWord("function");
        coach.skipSpace();
        
        
        // name
        let i = coach.i;
        let objectLink = coach.parseObjectLink();
        if ( 
            objectLink.link.length != 2 &&
            objectLink.link.length != 1
        ) {
            coach.i = i;
            coach.throwError("invalid function name " + objectLink.toString());
        }

        let schema = "public";
        let name = objectLink.link[0].toLowerCase();
        if ( objectLink.link.length == 2 ) {
            schema = name;
            name = objectLink.link[1].toLowerCase();
        }
        
        this.schema = schema;
        this.name = name;
        coach.skipSpace();
        
        // arguments
        coach.expect("(");
        coach.skipSpace();

        if ( coach.isWord() ) {
            this.args = coach.parseComma("PgArgument", {default: true});
        } else {
            this.args = [];
        }

        // check defaults
        // cannot create function with args:
        // (a integer default null, b integer)
        let hasDefault = false;
        this.args.forEach(arg => {
            if ( arg.default ) {
                hasDefault = true;
                return;
            }

            if ( !arg.default && hasDefault ) {
                coach.throwError("input parameters after one with a default value must also have defaults");
            }
        });

        coach.skipSpace();
        coach.expect(")");
        coach.skipSpace();

        // returns
        coach.expectWord("returns");
        coach.skipSpace();
        
        this.returns = coach.parsePgReturns();
        coach.skipSpace();

        if ( coach.isWord("language") ) {
            this.language = this.parseLanguage(coach);
            coach.skipSpace();
        }
        
        coach.expectWord("as");
        coach.skipSpace();

        // body
        this.body = coach.parsePgString();
        coach.skipSpace();

        if ( coach.isWord("language") ) {
            this.language = this.parseLanguage(coach);
            coach.skipSpace();
        }

        if ( coach.isWord("immutable") ) {
            coach.expectWord("immutable");
            this.immutable = true;

            coach.skipSpace();
        }
        else if ( coach.isWord("stable") ) {
            coach.expectWord("stable");
            this.stable = true;

            coach.skipSpace();
        }
        else if ( coach.isWord("volatile") ) {
            coach.expectWord("volatile");
            this.volatile = true;

            coach.skipSpace();
        }
        else {
            this.volatile = true;
        }


        if ( coach.isWord("cost") ) {
            coach.expectWord("cost");
            coach.skipSpace();
            
            this.cost = coach.parsePgNumber();
            this.cost = +this.cost;
        }



        // validate arguments,
        // error on duplicate name
        let existsName = {};
        this.args.forEach(arg => {
            if ( arg.name in existsName ) {
                throw new Error(`parameter name "${ arg.name }" used more than once`);
            }

            existsName[ arg.name ] = true;
        });

        if ( this.returns.table ) {
            this.returns.table.forEach(arg => {
                if ( arg.name in existsName ) {
                    throw new Error(`parameter name "${ arg.name }" used more than once`);
                }

                existsName[ arg.name ] = true;
            });
        }
    }

    parseLanguage(coach) {
        coach.expectWord("language");
        coach.skipSpace();
        
        let language = coach.expectWord();
        language = language.toLowerCase();

        if ( language != "plpgsql" && language != "sql" ) {
            coach.throwError("expected language plpgsql or sql");
        }

        return language;
    }

    toString() {
        let additionalParams = "";

        if ( this.immutable ) {
            additionalParams += " immutable";
        }
        else if ( this.stable ) {
            additionalParams += " stable";
        }


        if ( this.cost != null ) {
            additionalParams += " cost " + this.cost;
        }
        

        return `create or replace function ${ this.schema }.${ this.name }(${
            this.args.map(arg =>
                arg.toString()
            ).join(", ")
        }) returns ${ 
            this.returns.toString() 
        } as ${ 
            this.body 
        } language ${ this.language }` + additionalParams;
    }

    static function2sql(func) {
        let returnsSql = func.returns;

        if ( func.returns.table ) {
            returnsSql = "table(" + (
                func.returns.table.map(arg =>
                    arg.name + " " + arg.type
                )
            ) + ")";
        }

        let argsSql = [];
        func.args.forEach(arg => {
            if ( arg.default ) {
                argsSql.push(`${ arg.name } ${ arg.type } default ${ arg.default }`);
            } else {
                argsSql.push(`${ arg.name } ${ arg.type }`);
            }
        });
        argsSql = argsSql.join(", ");

        return `create or replace 
        function ${ func.schema }.${ func.name }(${argsSql}) 
        returns ${ returnsSql } 
        as $body$${ func.body  }$body$
        language ${ func.language }`;
    }

    // public.some_func(bigint, text)
    static function2identifySql(func) {
        let args = func.args.filter(arg => 
            !arg.default
        );

        let argsSql = args.map(arg => 
            arg.type
        );
        
        argsSql = argsSql.join(", ");
        return `${ func.schema }.${ func.name }(${ argsSql })`;
    }

    static function2dropSql(func) {
        // public.some_func(bigint, text)
        let identifySql = CreateFunction.function2identifySql(func);

        return `drop function if exists ${ identifySql }`;
    }

    clone() {
        let clone = new CreateFunction();

        clone.schema = this.schema;
        clone.name = this.name;
        clone.language = this.language;

        clone.args = this.args.map(arg =>
            arg.clone()
        );

        clone.returns = this.returns.clone();
        clone.body = this.body.clone();

        if ( this.immutable ) {
            clone.immutable = true;
        }
        else if ( this.stable ) {
            clone.stable = true;
        }
        else {
            clone.volatile = true;
        }

        if ( this.cost != null ) {
            clone.cost = this.cost;
        }

        return clone;
    }

    toJSON() {
        let returns = this.returns.type;
        if ( this.returns.table ) {
            returns = {
                table: this.returns.table.map(arg => ({
                    name: arg.name,
                    type: arg.type
                }))
            };
        }

        let args = [];
        this.args.forEach(arg => {
            if ( arg.default ) {
                args.push({
                    name: arg.name,
                    type: arg.type,
                    default: arg.default
                });
            } else {
                args.push({
                    name: arg.name,
                    type: arg.type
                });
            }
        });

        let json = {
            schema: this.schema,
            name: this.name,
            body: this.body.content,
            language: this.language,
            args,
            returns
        };

        return json;
    }
}

module.exports = CreateFunction;