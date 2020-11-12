import { Cache, DatabaseFunction } from "./ast";
import { DatabaseTrigger } from "./ast";

export interface IState {
    functions: DatabaseFunction[];
    triggers: DatabaseTrigger[];
    cache?: Cache[];
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