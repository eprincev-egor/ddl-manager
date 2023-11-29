import { wrapText } from "../postgres/wrapText";
import { MAX_NAME_LENGTH } from "../postgres/constants";
import { Comment } from "./Comment";
import { uniq } from "lodash";
import { equalType, formatType } from "./Type";

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
    comment?: Comment;
}

export interface IDatabaseFunctionReturns {
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
    readonly cacheSignature?: string;
    readonly frozen?: boolean;
    
    readonly immutable?: boolean;
    readonly returnsNullOnNull?: boolean;
    readonly stable?: boolean;
    readonly strict?: boolean;
    readonly parallel?: ("safe" | "unsafe" | "restricted")[];
    readonly cost?: number;
    private assignedColumns?: string[];

    constructor(json: IDatabaseFunctionParams) {
        Object.assign(this, json);
        this.language = json.language || "plpgsql";

        this.name = this.name.slice(0, MAX_NAME_LENGTH);

        this.comment = json.comment || Comment.fromFs({
            objectType: "function"
        });
        this.cacheSignature = this.comment.cacheSignature;
        this.frozen = this.comment.frozen;
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

            // null == undefined
            // tslint:disable-next-line: triple-equals
            String(this.parallel) == String(otherFunc.parallel) &&

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

    findAssignColumns() {
        if ( this.assignedColumns ) {
            return this.assignedColumns;
        }
        const body = this.body
            .replace(/--[^\n\r]+/g, "")
            .replace(/\/\*.+?\*\//g, "");

        const matches = body.match(/(begin|then|loop|;)\s*;?\s*new\.(\w+)\s*=/g) || [];
        const assignedColumns = matches.map(str =>
            str
                .replace(/^((begin|then|loop)\s*;?|;)\s*new\./, "")
                .replace(/\s*=$/, "")
                .toLowerCase()
        );

        this.assignedColumns = uniq(assignedColumns).sort();
        return this.assignedColumns;
    }

    toSQL(params: {
        body?: string;
        immutable?: boolean;
        stable?: boolean;
    } = {}) {
        let additionalParams = "";
        
        if ( coalesce(params.immutable, this.immutable) ) {
            additionalParams += " immutable";
        }
        else if ( coalesce(params.stable, this.stable) ) {
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
returns ${ returnsSql }${ additionalParams } as ${ wrapText(coalesce(params.body, this.body), "body") }
language ${this.language}
    `.trim();
    }

    toSQLWithLog() {
        const DONT_LOG_FUNCS = [
            "log",
            "cm_build_array_for",
            "get_options"
        ];
        if ( DONT_LOG_FUNCS.includes(this.name) || this.language === "sql" ) {
            return this.toSQL();
        }
        
        // вставляем в самый первый begin
        let body = (" " + this.body).replace(
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
        
        const sql = this.toSQL({
            body,
            // INSERT is not allowed in a non-volatile function
            immutable: false,
            stable: false
        });
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
        equalType(argA.type, argB.type) &&
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

    someDefault = someDefault.replace(/\s*::\s*([\w\s]+|numeric\([\d\s,]+\))(\[])?$/, "");

    const type = formatType(someArg.type);
    someDefault += "::" + type;

    if ( someDefault.startsWith("{}::") ) {
        someDefault = someDefault.replace("{}::", "'{}'::");
    }
    
    return someDefault;
}


function equalReturns(returnsA: IDatabaseFunctionReturns, returnsB: IDatabaseFunctionReturns) {
    return (
        equalType(returnsA.type, returnsB.type) &&
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

function coalesce<T>(...values: (T | null | undefined)[]): T {
    return values.find(value => value != null) as T;
}