import { CreateFunction, CreateTrigger } from "grapeql-lang";

// TODO: use DatabaseFunction class
export type DatabaseFunctionType = CreateFunction["TJson"] & {
    returns: {
        type?: string;
        setof?: boolean;
        table?: any[];
    };
    freeze?: boolean;
    comment?: string;
};
export type DatabaseTriggerType = CreateTrigger["TJson"] & {
    freeze?: boolean;
    comment?: string;
    procedure: {
        schema: string;
        name: string;
        args: string[];
    };
    table: {
        schema: string;
        name: string;
    };
};

export interface IDatabaseDriver {
    loadFunctions(): Promise<DatabaseFunctionType[]>;
    loadTriggers(): Promise<DatabaseTriggerType[]>;
}