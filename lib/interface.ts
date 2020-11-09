import { DatabaseFunction } from "./ast/DatabaseFunction";
import { DatabaseTrigger } from "./ast/DatabaseTrigger";

export interface IState {
    functions: DatabaseFunction[];
    triggers: DatabaseTrigger[];
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