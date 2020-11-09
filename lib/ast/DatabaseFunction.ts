import { wrapText } from "../database/postgres/wrapText";

export interface IDatabaseFunctionParams {
    schema: string;
    name: string;
    args: IDatabaseFunctionArgument[];
    returns: IDatabaseFunctionReturns;
    body: string;
    language?: "plpgsql" | "sql";
    immutable?: boolean;
    returnsNullOnNull?: boolean;
    stable?: boolean;
    strict?: boolean;
    parallel?: ("safe" | "unsafe" | "restricted")[];
    cost?: number;
    comment?: string;
}

interface IDatabaseFunctionReturns {
    setof?: boolean;
    table?: IDatabaseFunctionArgument[];
    type?: string;
}

interface IDatabaseFunctionArgument {
    out?: boolean;
    in?: boolean;
    name?: string;
    type: string;
    default?: string;
}

export class DatabaseFunction  {
    schema!: string;
    name!: string;
    returns!: IDatabaseFunctionReturns;
    args!: IDatabaseFunctionArgument[];
    body!: string;
    language: "plpgsql" | "sql";    
    
    immutable?: boolean;
    returnsNullOnNull?: boolean;
    stable?: boolean;
    strict?: boolean;
    parallel?: ("safe" | "unsafe" | "restricted")[];
    cost?: number;

    frozen?: boolean;
    comment?: string;

    constructor(json: IDatabaseFunctionParams) {
        Object.assign(this, json);
        this.language = json.language || "plpgsql";
    }

    getSignature() {
        const argsTypes = this.args
            .filter((arg: any) => 
                !arg.out
            )
            .map((arg: any) => 
                arg.type
            );

        return `${ this.schema }.${ this.name }(${ argsTypes.join(", ") })`;
    }

    toSQL() {
        let additionalParams = "";

        additionalParams += " language ";
        additionalParams += this.language;
        
        if ( this.immutable ) {
            additionalParams += " immutable";
        }
        else if ( this.stable ) {
            additionalParams += " stable";
        }

        if ( this.returnsNullOnNull ) {
            additionalParams += " returns null on null input";
        }
        else if ( this.strict ) {
            additionalParams += " strict";
        }


        if ( this.parallel ) {
            additionalParams += " parallel ";
            additionalParams += this.parallel;
        }

        if ( this.cost != null ) {
            additionalParams += " cost " + this.cost;
        }

        
        const returnsSql = returns2sql(this.returns);

        let argsSql = this.args.map((arg: any) => 
            "    " + arg2sql(arg)
        ).join(",\n");

        if ( this.args.length ) {
            argsSql = "\n" + argsSql + "\n";
        }

        // отступов не должно быть!
        // иначе DDLManager.dump будет писать некрасивый код
        return `
create or replace function ${ this.schema }.${ this.name }(${argsSql}) 
returns ${ returnsSql } 
${ additionalParams }
as ${ wrapText(this.body) }
    `.trim();
    }

    toSQLWithComment() {
        let sql = this.toSQL();

        if ( this.comment ) {
            sql += ";\n";
            sql += "\n";

            sql += `comment on function ${this.getSignature()} is ${ wrapText(this.comment) }`;
        }

        return sql;
    }
}


function returns2sql(returns: IDatabaseFunctionReturns) {
    let out = "";

    if ( returns.setof ) {
        out += "setof ";
    }

    if ( returns.table ) {
        out += `table(${ 
            returns.table.map((arg: any) => 
                arg2sql(arg)
            ).join(", ") 
        })`;
    } else {
        out += returns.type;
    }

    return out;
}

function arg2sql(arg: IDatabaseFunctionArgument) {
    let out = "";

    if ( arg.out ) {
        out += "out ";
    }
    else if ( arg.in ) {
        out += "in ";
    }

    if ( arg.name ) {
        out += arg.name;
        out += " ";
    }

    out += arg.type;

    if ( arg.default ) {
        out += " default ";
        out += arg.default;
    }

    return out;
}
