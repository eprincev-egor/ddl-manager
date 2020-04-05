import { FunctionModel } from "../objects/FunctionModel";
import { TableModel } from "../objects/TableModel";
import { TriggerModel } from "../objects/TriggerModel";
// import { IFunctionCommandDBDriver } from "../migration/commands/FunctionCommandModel";
// import { ITriggerCommandDBDriver } from "../migration/commands/TriggerCommandModel";

export abstract class DBDriver
// implements 
//     IFunctionCommandDBDriver,
//     ITriggerCommandDBDriver 
{
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
    abstract loadTriggers(): Promise<TriggerModel[]>;
    abstract loadTables(): Promise<TableModel[]>;

    abstract dropFunction(functionModel: FunctionModel): Promise<void>;
    abstract createFunction(functionModel: FunctionModel): Promise<void>;

    abstract dropTrigger(triggerModel: TriggerModel): Promise<void>;
    abstract createTrigger(triggerModel: TriggerModel): Promise<void>;

    abstract connect(): Promise<void>;
    abstract end(): Promise<void>;
}