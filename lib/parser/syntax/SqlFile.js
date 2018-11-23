"use strict";

const Syntax = require("./Syntax");

class SqlFile extends Syntax {
    parse(coach) {
        coach.skipSpace();
        this.function = coach.parseCreateFunction();

        coach.skipSpace();
        if ( coach.is(";") ) {
            coach.expect(";");
            coach.skipSpace();

            if ( coach.isCreateTrigger() ) {
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
            
                this.trigger = trigger;
            }
        }
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

        if ( this.trigger ) {
            clone.trigger = this.trigger.clone();
        }
        
        return clone;
    }
    
    toString() {
        let out = this.function.toString();

        if ( this.trigger ) {
            out += ";";
            out += this.trigger.toString();
        }

        return out;
    }

    toJSON() {
        let json = {
            function: this.function.toJSON()
        };

        if ( this.trigger ) {
            json.trigger = this.trigger.toJSON();
        }

        return json;
    }
}

module.exports = SqlFile;
