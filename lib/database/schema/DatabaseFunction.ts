import { wrapText } from "../postgres/wrapText";
import { MAX_NAME_LENGTH } from "../postgres/constants";
import { Comment } from "./Comment";

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
    comment?: Comment | string;
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
    readonly schema!: string;
    readonly name!: string;
    readonly returns!: IDatabaseFunctionReturns;
    readonly args!: IDatabaseFunctionArgument[];
    readonly body!: string;
    readonly language: "plpgsql" | "sql";

    readonly comment!: Comment;
    readonly frozen: boolean;
    readonly cacheSignature?: string;
    
    readonly immutable?: boolean;
    readonly returnsNullOnNull?: boolean;
    readonly stable?: boolean;
    readonly strict?: boolean;
    readonly parallel?: ("safe" | "unsafe" | "restricted")[];
    readonly cost?: number;


    constructor(json: IDatabaseFunctionParams) {
        Object.assign(this, json);
        this.language = json.language || "plpgsql";

        // if ( this.name.length > MAX_NAME_LENGTH ) {
        //     // tslint:disable-next-line: no-console
        //     console.error(`name "${this.name}" too long (> 64 symbols)`);
        // }
        this.name = this.name.slice(0, MAX_NAME_LENGTH);

        if ( !json.comment ) {
            this.comment = Comment.fromFs({
                objectType: "function"
            });
        }
        else if ( typeof json.comment === "string" ) {
            this.comment = Comment.fromFs({
                objectType: "function",
                dev: json.comment
            });
        }
        
        this.frozen = this.comment.frozen || false;
        this.cacheSignature = this.comment.cacheSignature;
    }

    equalName(schemaName: string): boolean {
        if ( !schemaName.includes(".") ) {
            schemaName = "public." + schemaName;
        }
        return this.schema + "." + this.name === schemaName;
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

            this.comment.equal(otherFunc.comment)
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

    toSQL(body = this.body) {
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
returns ${ returnsSql }${ additionalParams } as ${ wrapText(body, "body") }
language ${this.language}
    `.trim();
    }

    toSQLWithLog() {
        let body = this.body;

        const DONT_LOG_FUNCS = [
            "log",
            "get_options"
        ];
        if (
            !DONT_LOG_FUNCS.includes(this.name) && 
            this.language != "sql" 
        ) {
            // вставляем в самый первый begin
            body = (" " + body).replace(
                /(([^\w_])begin[^\w_])/i, 
                `$2
                declare ___call_id integer;
                begin
                insert into system_calls (
                    tid,
                    func_name,
                    call_time
                ) values (
                    txid_current(),
                    '${ this.schema }.${ this.name }',
                    extract(epoch from timeofday()::timestamp without time zone)
                )
                returning id into ___call_id;
                `
            );

            // перед каждым return
            body = body.replace(
                /([^\w_]return[^\w_]|[^\w_]end\s*;?\s*(\$\w+\$)?\s*$)/ig, 
                `
                insert into system_calls (
                    tid,
                    end_time,
                    end_id
                ) values (
                    txid_current(),
                    extract(epoch from timeofday()::timestamp without time zone),
                    ___call_id
                )
                on conflict (end_id)
                do update set
                    end_time = excluded.end_time,
                    id = excluded.id;
                $1
                `
            );
        }
        
        const sql = this.toSQL(body);
        return sql;
    }

    toSQLWithComment() {
        let sql = this.toSQL();

        if ( this.comment.dev ) {
            sql += ";\n";
            sql += "\n";

            sql += `comment on function ${this.getSignature()} is ${ wrapText(this.comment.dev) }`;
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

    const defaultA = formatDefault(argA);
    const defaultB = formatDefault(argB);
    return defaultA === defaultB;
}

function formatDefault(someArg: IDatabaseFunctionArgument) {
    let someDefault = ("" + someArg.default).trim().toLowerCase();

    someDefault = someDefault.replace(/\s*::\s*([\w\s]+|numeric\([\d\s,]+\))$/, "");

    const type = formatType(someArg.type);
    someDefault += "::" + type;

    if ( someDefault.startsWith("{}::") ) {
        someDefault = someDefault.replace("{}::", "'{}'::");
    }
    
    return someDefault;
}

function formatType(someType?: string) {
    if ( !someType ) {
        return null;
    }

    someType = someType.trim().toLowerCase().replace(/\s+/g, " ");

    if ( someType.startsWith("numeric") ) {
        return "numeric";
    }

    if ( someType === "timestamp" ) {
        return "timestamp without time zone";
    }

    return someType;
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