import { CreateTrigger } from "grapeql-lang";
import { DatabaseFunction } from "./ast/DatabaseFunction";

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
    functions: DatabaseFunction[];
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