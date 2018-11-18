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

        coach.skipSpace();
        coach.expect(")");
        coach.skipSpace();

        // returns
        coach.expectWord("returns");
        coach.skipSpace();
        
        this.returns = coach.parsePgReturns();
        coach.skipSpace();
        
        coach.expectWord("as");
        coach.skipSpace();

        // body
        this.body = coach.parsePgString();
        coach.skipSpace();

        coach.expectWord("language");
        coach.skipSpace();
        coach.expectWord("plpgsql");
    }

    toString() {
        return `create or replace function ${ this.schema }.${ this.name }(${
            this.args.map(arg =>
                arg.toString()
            ).join(", ")
        }) returns ${ 
            this.returns.toString() 
        } as ${ 
            this.body 
        } language plpgsql`;
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
        language plpgsql`;
    }


    clone() {
        let clone = new CreateFunction();

        clone.schema = this.schema;
        clone.name = this.name;

        clone.args = this.args.map(arg =>
            arg.clone()
        );

        clone.returns = this.returns.clone();
        clone.body = this.body.clone();

        return clone;
    }
}

module.exports = CreateFunction;