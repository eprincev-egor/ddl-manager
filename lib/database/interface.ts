import { CreateFunction, CreateTrigger } from "grapeql-lang";

// TODO: use DatabaseFunction class
export type DatabaseFunctionType = CreateFunction["TJson"] & {
    freeze?: boolean;
    comment?: string;
};
export type DatabaseTriggerType = CreateTrigger["TJson"] & {
    freeze?: boolean;
    comment?: string;
    table: {
        schema: string;
        name: string;
    };
};

export interface IDatabaseDriver {
    loadFunctions(): Promise<DatabaseFunctionType[]>;
    loadTriggers(): Promise<DatabaseTriggerType[]>;
}