import { wrapText } from "../postgres/wrapText";
import { MAX_NAME_LENGTH } from "../postgres/constants";
import { TableID } from "./TableID";
import { Comment } from "./Comment";

export interface IDatabaseTriggerParams {
    name: string;
    table: TableID;
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

    comment?: Comment | string;
}

export class DatabaseTrigger {
    readonly name!: string;
    readonly procedure!: {
        schema: string;
        name: string;
        args: string[];
    };
    readonly table!: TableID;

    readonly comment!: Comment;
    readonly frozen?: boolean;
    readonly cacheSignature?: string;

    readonly before?: boolean;
    readonly after?: boolean;
    readonly insert?: boolean;
    readonly delete?: boolean;
    readonly update?: boolean;
    readonly updateOf?: string[];
    readonly when?: string;
    
    readonly constraint?: boolean;
    readonly deferrable?: boolean;
    readonly notDeferrable?: boolean;
    readonly statement?: boolean;
    readonly initially?: "immediate" | "deferred";

    constructor(json: IDatabaseTriggerParams) {
        Object.assign(this, json);
        // tslint:disable-next-line: no-console
        if ( this.name.length > MAX_NAME_LENGTH ) {
            console.error(`name "${this.name}" too long (> 64 symbols)`);
        }
        this.name = this.name.slice(0, MAX_NAME_LENGTH);

        if ( !(this.table instanceof TableID) ) {
            const tableJson = this.table as {schema: string, name: string};
            this.table = new TableID(
                tableJson.schema,
                tableJson.name
            );
        }

        if ( !json.comment ) {
            this.comment = Comment.fromFs({
                objectType: "trigger"
            });
        }
        else if ( typeof json.comment === "string" ) {
            this.comment = Comment.fromFs({
                objectType: "trigger",
                dev: json.comment
            });
        }
        this.frozen = this.comment.frozen;
        this.cacheSignature = this.comment.cacheSignature;
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
            this.comment.equal(otherTrigger.comment) &&
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

        if ( this.comment.dev ) {
            sql += ";\n";
            sql += "\n";

            sql += `comment on trigger ${this.getSignature()} is ${ wrapText(this.comment.dev) }`;
        }

        return sql;
    }
}