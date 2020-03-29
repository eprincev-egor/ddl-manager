import { FunctionModel } from "../objects/FunctionModel";

export abstract class DBDriver {
    protected options: {
        host: string;
        port: number;
        database: string;
        user: string;
        password: string;
    };

    constructor(options: DBDriver["options"]) {
        this.options = options;
    }

    abstract loadFunctions(): Promise<FunctionModel[]>;
    abstract connect(): Promise<void>;
}