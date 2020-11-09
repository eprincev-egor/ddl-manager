import { DatabaseFunctionType } from "../interface";

export class DatabaseFunction  {
    schema!: string;
    name!: string;
    returns!: {
        type?: string;
        setof?: boolean;
        table?: any[];
    };
    frozen?: boolean;
    comment?: string;
    args!: any[];

    constructor(json: DatabaseFunctionType) {
        Object.assign(this, json);
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