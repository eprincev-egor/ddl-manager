import {
    Table,
    Cache
} from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { CommutativeTriggerBuilder } from "./CommutativeTriggerBuilder";
import { Database as DatabaseStructure } from "../schema/Database";
import { buildFromAndWhere } from "../processor/buildFromAndWhere";
import { UniversalTriggerBuilder } from "./UniversalTriggerBuilder";
import { findJoinsMeta } from "../processor/findJoinsMeta";
import { JoinedCommutativeTriggerBuilder } from "./JoinedCommutativeTriggerBuilder";
import { CacheContext } from "./CacheContext";

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

        const Builder = this.chooseConstructor(triggerTable);

        const context = new CacheContext(
            this.cache,
            triggerTable,
            triggerTableColumns,
            this.databaseStructure
        );
        const builder = new Builder(
            context
        );
        return builder;
    }

    private chooseConstructor(triggerTable: Table) {

        const {from} = buildFromAndWhere(
            this.cache,
            triggerTable
        );
        const joins = findJoinsMeta(this.cache.select);

        if ( from.length > 1 ) {
            return UniversalTriggerBuilder;
        }
        else if ( joins.length ) {
            return JoinedCommutativeTriggerBuilder;
        }
        else {
            return CommutativeTriggerBuilder;
        }
    }
}