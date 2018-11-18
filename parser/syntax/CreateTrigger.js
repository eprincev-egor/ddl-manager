"use strict";

// НЕ ПОЛНАЯ ВЕРСИЯ СИНТАКСИСА!!
// описание полной здесь:
// https://www.postgresql.org/docs/9.5/sql-createtrigger.html

const Syntax = require("./Syntax");

class CreateTrigger extends Syntax {
    is(coach) {
        return coach.is(/create(\s+constraint)?\s+trigger/i);
    }

    parse(coach) {
        coach.expectWord("create");
        coach.skipSpace();

        // if ( coach.isWord("constraint") ) {
        //     coach.expectWord("constraint");
        //     coach.skipSpace();
        //     this.isConstraint = true;
        // }

        coach.expectWord("trigger");
        coach.skipSpace();

        this.name = coach.readWord().toLowerCase();
        coach.skipSpace();
        

        if ( coach.isWord("before") ) {
            coach.expectWord("before");
            this.before = true;
        }
        else {
            coach.expectWord("after");
            this.after = true;
        }
        coach.skipSpace();

        this.parseEvents(coach);
        coach.skipSpace();
        
        

        coach.expectWord("on");
        coach.skipSpace();
        
        this.table = coach.parseSchemaName();
        coach.skipSpace();

        coach.expectWord("for");
        coach.skipSpace();
        coach.expectWord("each");
        coach.skipSpace();
        coach.expectWord("row");
        coach.skipSpace();

        if ( coach.isWord("when") ) {
            coach.expectWord("when");
            coach.skipSpace();

            coach.expect("(");
            coach.skipSpace();

            this.when = coach.parseExpression();
            this.when = this.when.toString();

            coach.skipSpace();
            coach.expect(")");
            coach.skipSpace();
        }

        coach.expectWord("execute");
        coach.skipSpace();
        coach.expectWord("procedure");
        coach.skipSpace();

        this.procedure = coach.parseSchemaName();
        coach.skipSpace();
        coach.expect("(");
        coach.skipSpace();
        coach.expect(")");
    }

    parseEvents(coach) {
        if ( coach.isWord("insert") ) {
            coach.expectWord("insert");
            this.insert = true;
        }
        else if ( coach.isWord("delete") ) {
            coach.expectWord("delete");
            this.delete = true;
        }
        else {
            coach.expectWord("update");
            this.update = true;

            coach.skipSpace();
            if ( coach.isWord("of") ) {
                coach.expectWord("of");
                coach.skipSpace();

                let columns = coach.parseComma("ObjectName");
                columns = columns.map(objectName =>
                    objectName.toLowerCase()
                );

                this.update = columns;
            }
        }

        coach.skipSpace();
        if ( coach.isWord("or") ) {
            coach.expectWord("or");
            coach.skipSpace();

            this.parseEvents(coach);
        }
    }

    clone() {
        let clone = new CreateTrigger();

        clone.name = this.name;
        if ( this.before ) {
            clone.before = true;
        }
        else if ( this.after ) {
            clone.after = true;
        }
        
        if ( this.insert ) {
            clone.insert = true;
        }
        if ( this.delete ) {
            clone.delete = true;
        }
        if ( this.update ) {
            if ( this.update === true ) {
                clone.update = true;
            } else {
                clone.update = this.update.map(name => name);
            }
        }

        if ( this.when ) {
            clone.when = this.when;
        }
        
        clone.table = {
            schema: this.table.schema,
            name: this.table.name
        };

        clone.procedure = {
            schema: this.procedure.schema,
            name: this.procedure.name
        };

        return clone;
    }

    toString() {
        return CreateTrigger.trigger2sql(this);
    }

    static trigger2sql(trigger) {
        let out = `create trigger ${trigger.name} `;

        // after|before
        if ( trigger.before ) {
            out += "before";
        }
        else if ( trigger.after ) {
            out += "after";
        }
        out += " ";

        // insert or update of x or delete
        let events = [];
        if ( trigger.insert ) {
            events.push("insert");
        }
        if ( trigger.update ) {
            if ( trigger.update === true ) {
                events.push("update");
            } else {
                events.push(`update of ${ trigger.update.join(", ") }`);
            }
        }
        if ( trigger.delete ) {
            events.push("delete");
        }
        out += events.join(" or ");


        // table
        out += " on ";
        out += `${trigger.table.schema}.${trigger.table.name}`;

        out += " for each row ";

        if ( trigger.when ) {
            out += "when ( ";
            out += trigger.when;
            out += " ) ";
        }

        out += `execute procedure ${trigger.procedure.schema}.${trigger.procedure.name}()`;

        return out;
    }

    static trigger2dropSql(trigger) {
        return `drop trigger if exists ${trigger.name} 
        on ${ trigger.table.schema }.${ trigger.table.name }`;
    }

    toJSON() {
        let out = {
            table: {
                schema: this.table.schema,
                name: this.table.name
            },
            procedure: {
                schema: this.procedure.schema,
                name: this.procedure.name
            }
        };

        if ( this.before ) {
            out.before = true;
        }
        else if ( this.after ) {
            out.after = true;
        }

        if ( this.insert ) {
            out.insert = true;
        }
        if ( this.update ) {
            if ( this.update === true ) {
                out.update = true;
            } else {
                out.update = this.update.map(name => name);
            }
        }
        if ( this.delete ) {
            out.delete = true;
        }

        if ( this.when ) {
            out.when = this.when;
        }

        return out;
    }
}

module.exports = CreateTrigger;