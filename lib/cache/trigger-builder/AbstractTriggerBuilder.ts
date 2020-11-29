import {
    AbstractAstElement
} from "../../ast";
import { DatabaseFunction } from "../../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../../database/schema/DatabaseTrigger";
import { TableID } from "../../database/schema/TableID";
import { CacheContext } from "./CacheContext";
import { ConditionBuilder } from "./condition/ConditionBuilder";

export abstract class AbstractTriggerBuilder {
    protected readonly context: CacheContext;
    protected readonly conditionBuilder: ConditionBuilder;

    constructor(context: CacheContext) {
        this.context = context;
        this.conditionBuilder = new ConditionBuilder(
            context
        );
    }

    createTrigger(): {
        trigger: DatabaseTrigger;
        function: DatabaseFunction;
    } {
        return {
            trigger: this.createDatabaseTrigger(),
            function: this.createDatabaseFunction()
        };
    }

    private createDatabaseFunction() {

        const func = new DatabaseFunction({
            schema: "public",
            name: this.generateTriggerName(),
            body: "\n" + this.createBody().toSQL() + "\n",
            comment: "cache",
            args: [],
            returns: {type: "trigger"},
            cacheSignature: this.context.cache.getSignature()
        });
        return func;
    }

    protected createDatabaseTrigger() {
        
        const updateOfColumns = this.context.triggerTableColumns
            .filter(column =>  column !== "id" )
            .sort();
        
        const trigger = new DatabaseTrigger({
            name: this.generateTriggerName(),
            after: true,
            insert: true,
            delete: true,
            update: updateOfColumns.length > 0,
            updateOf: updateOfColumns,
            procedure: {
                schema: "public",
                name: this.generateTriggerName(),
                args: []
            },
            table: new TableID(
                this.context.triggerTable.schema || "public",
                this.context.triggerTable.name
            ),
            cacheSignature: this.context.cache.getSignature()
        });

        return trigger;
    }

    protected generateTriggerName() {
        const triggerName = [
            "cache",
            this.context.cache.name,
            "for",
            this.context.cache.for.table.name,
            "on",
            this.context.triggerTable.name
        ].join("_");
        return triggerName;
    }

    protected abstract createBody(): AbstractAstElement;
}