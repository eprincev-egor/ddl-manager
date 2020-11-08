import { CreateFunction, CreateTrigger } from "grapeql-lang";

// TODO: use DatabaseFunction class
export type DatabaseFunctionType = CreateFunction["TJson"] & {
    schema: string;
    name: string;
    returns: {
        type?: string;
        setof?: boolean;
        table?: any[];
    };
    frozen?: boolean;
    comment?: string;
};
export type DatabaseTriggerType = CreateTrigger["TJson"] & {
    frozen?: boolean;
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

// TODO: any => type
export interface IState {
    functions: DatabaseFunctionType[];
    triggers: DatabaseTriggerType[];
}

export interface IDiff {
    drop: IState;
    create: IState;
}

export interface IDatabaseDriver {
    loadState(): Promise<IState>;
}

export interface IFile {
    name: string;
    folder: string;
    path: string;
    content: IState;
}