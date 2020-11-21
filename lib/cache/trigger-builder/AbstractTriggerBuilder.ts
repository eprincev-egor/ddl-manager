import {
    Table,
    Cache,
    DatabaseTrigger,
    DatabaseFunction
} from "../../ast";
import { Database as DatabaseStructure } from "../schema/Database";

export abstract class AbstractTriggerBuilder {
    protected readonly cache: Cache;
    protected readonly databaseStructure: DatabaseStructure;
    protected readonly triggerTable: Table;
    protected readonly triggerTableColumns: string[];

    constructor(
        cache: Cache,
        databaseStructure: DatabaseStructure,
        triggerTable: Table,
        triggerTableColumns: string[]
    ) {
        this.cache = cache;
        this.databaseStructure = databaseStructure;
        this.triggerTable = triggerTable;
        this.triggerTableColumns = triggerTableColumns;
    }

    abstract createTrigger(): {
        trigger: DatabaseTrigger;
        function: DatabaseFunction;
    }
}