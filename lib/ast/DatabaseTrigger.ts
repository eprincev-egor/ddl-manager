
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
            out += "\nfor each statement ";
        } else {
            out += "\nfor each row ";
        }

        if ( this.when ) {
            out += "\nwhen ( ";
            out += this.when;
            out += " ) ";
        }

        out += `\nexecute procedure ${this.procedure.schema}.${this.procedure.name}()`;

        return out;
    }
}