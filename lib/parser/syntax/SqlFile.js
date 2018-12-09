"use strict";

const Syntax = require("./Syntax");

class SqlFile extends Syntax {
    parse(coach) {
        coach.skipSpace();
        this.function = coach.parseCreateFunction();

        this.parseTriggers( coach );
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
        
        return clone;
    }
    
    toString() {
        let out = this.function.toString();

        if ( this.triggers ) {
            this.triggers.forEach(trigger => {
                out += ";";
                out += trigger.toString();
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

        return json;
    }
}

module.exports = SqlFile;
