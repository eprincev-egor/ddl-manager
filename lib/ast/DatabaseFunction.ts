import { wrapText } from "../database/postgres/wrapText";
import { MAX_NAME_LENGTH } from "../database/postgres/constants";

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
    frozen?: boolean;
    comment?: string;
}

interface IDatabaseFunctionReturns {
    setof?: boolean;
    table?: IDatabaseFunctionArgument[];
    type?: string;
}

export interface IDatabaseFunctionArgument {
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

        if ( this.name.length > MAX_NAME_LENGTH ) {
            // tslint:disable-next-line: no-console
            console.error(`name "${this.name}" too long (> 64 symbols)`);
        }
        this.name = this.name.slice(0, MAX_NAME_LENGTH);
    }

    equal(otherFunc: DatabaseFunction) {
        return (
            this.schema === otherFunc.schema &&
            this.name === otherFunc.name &&
            this.body === otherFunc.body &&

            this.args.length === otherFunc.args.length &&
            this.args.every((myArg, i) =>
                equalArgument(myArg, otherFunc.args[i])
            )
            &&
            equalReturns(this.returns, otherFunc.returns) &&

            this.language === otherFunc.language &&
            !!this.immutable === !!otherFunc.immutable &&
            !!this.returnsNullOnNull === !!otherFunc.returnsNullOnNull &&
            !!this.stable === !!otherFunc.stable &&
            !!this.strict === !!otherFunc.strict &&

            !!this.frozen === !!otherFunc.frozen &&

            // null == undefined
            // tslint:disable-next-line: triple-equals
            this.parallel == otherFunc.parallel &&

            // null == undefined
            // tslint:disable-next-line: triple-equals
            this.comment == otherFunc.comment
        );
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

        if ( additionalParams ) {
            additionalParams = "\n" + additionalParams + "\n";
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
create or replace function ${ 
    this.schema === "public" ? 
        "" : 
        this.schema + "." 
}${ this.name }(${argsSql})
returns ${ returnsSql }${ additionalParams } as ${ wrapText(this.body, "body") }
language ${this.language}
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

function equalArgument(argA: IDatabaseFunctionArgument, argB: IDatabaseFunctionArgument) {
    return (
        // null == undefined
        // tslint:disable-next-line: triple-equals
        argA.name == argB.name &&
        formatType(argA.type) === formatType(argB.type) &&
        equalArgumentDefault(argA, argB) &&
        !!argA.in === !!argB.in &&
        !!argA.out === !!argB.out
    );
}

function equalArgumentDefault(argA: IDatabaseFunctionArgument, argB: IDatabaseFunctionArgument) {
    if ( !argA.default && !argB.default ) {
        return true;
    }

    // null == undefined
    // tslint:disable-next-line: triple-equals
    if ( argA.default == argB.default ) {
        return true;
    }

    let defaultA = ("" + argA.default).trim();
    let defaultB = ("" + argB.default).trim();

    if ( defaultA === "null" ) {
        defaultA = "null::" + formatType(argA.type);
    }
    defaultA = defaultA.replace(/^null\s*::\s*/i, "null::");

    if ( defaultB === "null" ) {
        defaultB = "null::" + formatType(argB.type);
    }
    defaultB = defaultB.replace(/^null\s*::\s*/i, "null::");

    return defaultA === defaultB;
}

function formatType(someType?: string) {
    if ( !someType ) {
        return null;
    }

    if ( /^\s*numeric/i.test(someType) ) {
        return "numeric";
    }

    return someType.toLowerCase();
}

function equalReturns(returnsA: IDatabaseFunctionReturns, returnsB: IDatabaseFunctionReturns) {
    return (
        formatType(returnsA.type) === formatType(returnsB.type) &&
        !!returnsA.setof === !!returnsB.setof &&
        (
            returnsA.table && returnsB.table &&
            returnsA.table.every((argA, i) =>
                equalArgument(argA, (returnsB.table as any)[i])
            )
            ||
            // null == undefined
            // tslint:disable-next-line: triple-equals
            returnsA.table == returnsB.table
        )
    );
}