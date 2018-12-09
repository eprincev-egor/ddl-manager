"use strict";

const Syntax = require("./Syntax");
const PgArgument = require("./PgArgument");
const PgReturns = require("./PgReturns");

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

        // lang, cost, immutable
        this.parseFunctionInfo( coach );
        coach.skipSpace();
        
        coach.expectWord("as");
        coach.skipSpace();

        // body
        this.body = coach.parsePgString();
        coach.skipSpace();

        // lang, cost, immutable
        this.parseFunctionInfo( coach );

        // validate arguments,
        // error on duplicate name
        let existsName = {};
        this.args.forEach(arg => {
            if ( arg.name === false ) {
                return;
            }
            
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

    parseFunctionInfo(coach) {
        if ( coach.isWord("language") ) {
            coach.expectWord("language");
            coach.skipSpace();
            
            let i = coach.i;
            let language = coach.expectWord();
            language = language.toLowerCase();

            if ( language != "plpgsql" && language != "sql" ) {
                coach.i = i;
                coach.throwError("expected language plpgsql or sql");
            }

            this.language = language;
        }
        coach.skipSpace();

        if ( coach.isWord("immutable") ) {
            coach.expectWord("immutable");
            this.immutable = true;
        }
        else if ( coach.isWord("stable") ) {
            coach.expectWord("stable");
            this.stable = true;
        }
        else if ( coach.isWord("volatile") ) {
            coach.expectWord("volatile");
        }
        coach.skipSpace();

        // CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
        if ( coach.isWord("called") ) {
            coach.expectWord("called");
            coach.skipSpace();

            coach.expectWord("on");
            coach.skipSpace();

            coach.expectWord("null");
            coach.skipSpace();

            coach.expectWord("input");
            coach.skipSpace();
        }
        else if ( coach.isWord("returns") ) {
            coach.expectWord("returns");
            coach.skipSpace();

            coach.expectWord("null");
            coach.skipSpace();

            coach.expectWord("on");
            coach.skipSpace();

            coach.expectWord("null");
            coach.skipSpace();

            coach.expectWord("input");
            coach.skipSpace();

            this.returnsNullOnNull = true;
        }
        else if ( coach.isWord("strict") ) {
            coach.expectWord("strict");
            coach.skipSpace();

            this.strict = true;
        }


        if ( coach.isWord("parallel") ) {
            coach.expectWord("parallel");
            coach.skipSpace();

            let i = coach.i;
            this.parallel = coach.readWord();
            this.parallel = this.parallel.toLowerCase();

            if ( !["safe", "unsafe", "restricted"].includes(this.parallel) ) {
                coach.i = i;
                coach.throwError("parallel can be one of: safe, unsafe, restricted");
            }
        }
        coach.skipSpace();

        if ( coach.isWord("cost") ) {
            coach.expectWord("cost");
            coach.skipSpace();
            
            this.cost = coach.parsePgNumber();
            this.cost = +this.cost;
        }

        coach.skipSpace();
    }

    toString() {
        return CreateFunction.function2sql( this );
    }

    static function2sql(func) {
        let additionalParams = "";

        additionalParams += " language ";
        additionalParams += func.language;
        
        if ( func.immutable ) {
            additionalParams += " immutable";
        }
        else if ( func.stable ) {
            additionalParams += " stable";
        }

        if ( func.returnsNullOnNull ) {
            additionalParams += " returns null on null input";
        }
        else if ( func.strict ) {
            additionalParams += " strict";
        }


        if ( func.parallel ) {
            additionalParams += " parallel ";
            additionalParams += func.parallel;
        }

        if ( func.cost != null ) {
            additionalParams += " cost " + func.cost;
        }

        
        let returnsSql = PgReturns.returns2sql(func.returns, {
            lineBreak: true
        });

        let argsSql = func.args.map(arg => 
            "    " + PgArgument.arg2sql(arg)
        ).join(",\n");

        if ( func.args.length ) {
            argsSql = "\n" + argsSql + "\n";
        }


        let body = func.body;
        if ( body.content ) {
            body = body.toString();
        }
        else {
            body = `$body$${ body }$body$`;
        }

        // отступов не должно быть!
        // иначе DDLManager.dump будет писать некрасивый код
        return `
create or replace function ${ func.schema }.${ func.name }(${argsSql}) 
returns ${ returnsSql } 
${ additionalParams }
as ${ body }
        `.trim();
    }

    // public.some_func(bigint, text)
    static function2identifySql(func) {
        let identify = CreateFunction.function2identifyJson( func );

        let argsSql = identify.args.join(", ");
        return `${ identify.schema }.${ identify.name }(${ argsSql })`;
    }

    static function2identifyJson(func) {
        let args = func.args.filter(arg => (
            !arg.default &&
            !arg.out
        ));

        args = args.map(arg => 
            arg.type
        );
        
        return {
            schema: func.schema,
            name: func.name,
            args
        };
    }

    static function2dropSql(func) {
        // public.some_func(bigint, text)
        let identifySql = CreateFunction.function2identifySql(func);

        return `drop function if exists ${ identifySql }`;
    }

    static parseFunctionIdentify(coach) {
        let {schema, name} = coach.parseSchemaName();

        coach.skipSpace();
        coach.expect("(");
        coach.skipSpace();

        let args = coach.parseComma("DataType");
        args = args.map(arg =>
            arg.type
        );

        coach.skipSpace();
        coach.expect(")");

        return {
            schema,
            name,
            args
        };
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

        if ( this.returnsNullOnNull ) {
            clone.returnsNullOnNull = true;
        }
        if ( this.strict ) {
            clone.strict = true;
        }

        if ( this.parallel ) {
            clone.parallel = this.parallel;
        }

        if ( this.cost != null ) {
            clone.cost = this.cost;
        }

        return clone;
    }

    toJSON() {
        let json = {
            schema: this.schema,
            name: this.name,
            body: this.body.content,
            language: this.language,
            args: this.args.map(arg => 
                arg.toJSON()
            ),
            returns: this.returns.toJSON()
        };

        if ( this.immutable ) {
            json.immutable = true;
        }
        else if ( this.stable ) {
            json.stable = true;
        }

        if ( this.returnsNullOnNull ) {
            json.returnsNullOnNull = true;
        }
        if ( this.strict ) {
            json.strict = true;
        }

        if ( this.parallel ) {
            json.parallel = this.parallel;
        }

        if ( this.cost != null ) {
            json.cost = this.cost;
        }

        return json;
    }
}

module.exports = CreateFunction;