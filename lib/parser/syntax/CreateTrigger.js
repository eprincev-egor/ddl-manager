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

        if ( coach.isWord("constraint") ) {
            coach.expectWord("constraint");
            coach.skipSpace();
            this.constraint = true;
        }

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

        // NOT DEFERRABLE | [ DEFERRABLE ] [ INITIALLY IMMEDIATE | INITIALLY DEFERRED ]
        if ( coach.isWord("not") ) {
            coach.expectWord("not");
            coach.skipSpace();

            coach.expectWord("deferrable");
            coach.skipSpace();
            this.notDeferrable = true;
        }
        else if ( coach.isWord("deferrable") ) {
            coach.expectWord("deferrable");
            coach.skipSpace();

            this.deferrable = true;
        }
        if ( coach.isWord("initially") ) {
            coach.expectWord("initially");
            coach.skipSpace();
            
            if ( coach.isWord("immediate") ) {
                coach.expectWord("immediate");
                this.initially = "immediate";
            }
            else {
                coach.expectWord("deferred");
                this.initially = "deferred";
            }
            coach.skipSpace();
        }


        if ( coach.isWord("for") ) {
            coach.expectWord("for");
            coach.skipSpace();

            if ( coach.isWord("each") ) {
                coach.expectWord("each");
                coach.skipSpace();
            }

            if ( coach.isWord("row") ) {
                coach.expectWord("row");
            }
            else {
                coach.expectWord("statement");
                this.statement = true;
            }
            coach.skipSpace();
        }
        

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

                this.update = columns.sort();
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

        if ( this.statement ) {
            clone.statement = true;
        }

        if ( this.constraint ) {
            clone.constraint = true;
        }

        if ( this.notDeferrable ) {
            clone.notDeferrable = true;
        }
        if ( this.deferrable ) {
            clone.deferrable = true;
        }
        if ( this.initially ) {
            clone.initially = this.initially;
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
        let out = "create ";

        if ( trigger.constraint ) {
            out += "constraint ";
        }
        
        out += `trigger ${trigger.name}\n`;

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
        out += "\non ";
        out += `${trigger.table.schema}.${trigger.table.name}`;

        if ( trigger.notDeferrable ) {
            out += " not deferrable";
        }
        if ( trigger.deferrable ) {
            out += " deferrable";
        }
        if ( trigger.initially ) {
            out += " initially ";
            out += trigger.initially;
        }


        if ( trigger.statement ) {
            out += "\nfor each statement ";
        } else {
            out += "\nfor each row ";
        }

        if ( trigger.when ) {
            out += "\nwhen ( ";
            out += trigger.when;
            out += " ) ";
        }

        out += `\nexecute procedure ${trigger.procedure.schema}.${trigger.procedure.name}()`;

        return out;
    }

    static trigger2dropSql(trigger) {
        let identifySql = CreateTrigger.trigger2identifySql(trigger);
        return `drop trigger if exists ${ identifySql }`;
    }

    // some_trigger on public.test
    static trigger2identifySql(trigger) {
        return `${trigger.name} on ${ trigger.table.schema }.${ trigger.table.name }`;
    }

    toJSON() {
        let out = {
            name: this.name,
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
                out.update = this.update.map(name => name).sort();
            }
        }
        if ( this.delete ) {
            out.delete = true;
        }

        if ( this.when ) {
            out.when = this.when;
        }

        if ( this.statement ) {
            out.statement = true;
        }

        if ( this.constraint ) {
            out.constraint = true;
        }

        if ( this.notDeferrable ) {
            out.notDeferrable = true;
        }
        if ( this.deferrable ) {
            out.deferrable = true;
        }
        if ( this.initially ) {
            out.initially = this.initially;
        }

        return out;
    }
}

module.exports = CreateTrigger;