import { wrapText } from "../database/postgres/wrapText";
import { MAX_NAME_LENGTH } from "../database/postgres/constants";

export interface IDatabaseTriggerParams {
    name: string;
    table: {
        schema: string;
        name: string;
    };
    procedure: {
        schema: string;
        name: string;
        args: string[];
    }

    before?: boolean;
    after?: boolean;
    insert?: boolean;
    delete?: boolean;
    update?: boolean;
    updateOf?: string[];
    when?: string;
    
    constraint?: boolean;
    deferrable?: boolean;
    notDeferrable?: boolean;
    statement?: boolean;
    initially?: "immediate" | "deferred";

    comment?: string;
    frozen?: boolean;
}

export class DatabaseTrigger {
    name!: string;
    procedure!: {
        schema: string;
        name: string;
        args: string[];
    };
    table!: {
        schema: string;
        name: string;
    };

    before?: boolean;
    after?: boolean;
    insert?: boolean;
    delete?: boolean;
    update?: boolean;
    updateOf?: string[];
    when?: string;
    
    constraint?: boolean;
    deferrable?: boolean;
    notDeferrable?: boolean;
    statement?: boolean;
    initially?: "immediate" | "deferred";

    frozen?: boolean;
    comment?: string;

    constructor(json: IDatabaseTriggerParams) {
        Object.assign(this, json);
        if ( this.name.length > MAX_NAME_LENGTH ) {
            // tslint:disable-next-line: no-console
            console.error(`name "${this.name}" too long (> 64 symbols)`);
        }
        this.name = this.name.slice(0, MAX_NAME_LENGTH);
    }

    equal(otherTrigger: DatabaseTrigger) {
        return (
            this.name === otherTrigger.name &&
            this.table.schema === otherTrigger.table.schema &&
            this.table.name === otherTrigger.table.name &&
            this.procedure.schema === otherTrigger.procedure.schema &&
            this.procedure.name === otherTrigger.procedure.name &&
            this.procedure.args.join(",") === otherTrigger.procedure.args.join(",") &&
            !!this.before === !!otherTrigger.before &&
            !!this.after === !!otherTrigger.after &&
            !!this.insert === !!otherTrigger.insert &&
            !!this.delete === !!otherTrigger.delete &&
            !!this.update === !!otherTrigger.update &&
            (
                this.updateOf && otherTrigger.updateOf &&
                this.updateOf.join(",") === otherTrigger.updateOf.join(",")
                ||
                // null == undefined
                // tslint:disable-next-line: triple-equals
                this.updateOf == otherTrigger.updateOf
            ) &&
            // null == undefined
            // tslint:disable-next-line: triple-equals
            this.comment == otherTrigger.comment &&
            // null == undefined
            // tslint:disable-next-line: triple-equals
            this.when == otherTrigger.when &&
            // null == undefined == false
            !!this.frozen === !!otherTrigger.frozen
            &&
            !!this.constraint === !!otherTrigger.constraint &&
            !!this.deferrable === !!otherTrigger.deferrable &&
            !!this.notDeferrable === !!otherTrigger.notDeferrable &&
            !!this.statement === !!otherTrigger.statement &&
            // null == undefined
            // tslint:disable-next-line: triple-equals
            this.initially == otherTrigger.initially
        
        );
    }

    getSignature() {
        return `${this.name} on ${ this.table.schema }.${ this.table.name }`;
    }

    toSQL() {
        let out = "create ";

        if ( this.constraint ) {
            out += "constraint ";
        }
        
        out += `trigger ${this.name}\n`;

        // after|before
        if ( this.before ) {
            out += "before";
        }
        else if ( this.after ) {
            out += "after";
        }
        out += " ";

        // insert or update of x or delete
        const events: string[] = [];
        if ( this.insert ) {
            events.push("insert");
        }
        if ( this.update ) {
            if ( this.updateOf && this.updateOf.length ) {
                events.push(`update of ${ this.updateOf.join(", ") }`);
            }
            else if ( this.update === true ) {
                events.push("update");
            }
        }
        if ( this.delete ) {
            events.push("delete");
        }
        out += events.join(" or ");


        // table
        out += "\non ";
        out += `${this.table.schema}.${this.table.name}`;

        if ( this.notDeferrable ) {
            out += " not deferrable";
        }
        if ( this.deferrable ) {
            out += " deferrable";
        }
        if ( this.initially ) {
            out += " initially ";
            out += this.initially;
        }


        if ( this.statement ) {
            out += "\nfor each statement";
        } else {
            out += "\nfor each row";
        }

        if ( this.when ) {
            out += "\nwhen ( ";
            out += this.when;
            out += " ) ";
        }

        out += `\nexecute procedure ${ 
            this.procedure.schema === "public" ? 
                "" : 
                this.procedure.schema + "." 
        }${ this.procedure.name }()`;

        return out;
    }

    toSQLWithComment() {
        let sql = this.toSQL();

        if ( this.comment ) {
            sql += ";\n";
            sql += "\n";

            sql += `comment on trigger ${this.getSignature()} is ${ wrapText(this.comment) }`;
        }

        return sql;
    }
}