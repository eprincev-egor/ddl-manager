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
}