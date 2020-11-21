import {
    Table,
    Cache
} from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { TriggerBuilder } from "./TriggerBuilder";
import { Database as DatabaseStructure } from "../schema/Database";

export class TriggerBuilderFactory {
    private readonly cache: Cache;
    private readonly databaseStructure: DatabaseStructure;

    constructor(
        cache: Cache,
        databaseStructure: DatabaseStructure,
    ) {
        this.cache = cache;
        this.databaseStructure = databaseStructure;
    }

    createBuilder(
        triggerTable: Table,
        triggerTableColumns: string[]
    ): AbstractTriggerBuilder {
        return new TriggerBuilder(
            this.cache,
            this.databaseStructure,
            triggerTable,
            triggerTableColumns
        );
    }
}