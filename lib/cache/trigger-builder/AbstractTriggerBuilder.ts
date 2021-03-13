import {
    AbstractAstElement,
    Expression
} from "../../ast";
import { Comment } from "../../database/schema/Comment";
import { DatabaseFunction } from "../../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../../database/schema/DatabaseTrigger";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { DeltaSetItemsFactory } from "../aggregator/DeltaSetItemsFactory";
import { SetItemsFactory } from "../aggregator/SetItemsFactory";
import { CacheContext } from "./CacheContext";
import { ConditionBuilder } from "./condition/ConditionBuilder";

export interface ICacheTrigger {
    trigger: DatabaseTrigger;
    function: DatabaseFunction;
}

export abstract class AbstractTriggerBuilder {
    protected readonly context: CacheContext;
    protected readonly conditions: ConditionBuilder;
    protected readonly setItems: SetItemsFactory;
    protected readonly deltaSetItems: DeltaSetItemsFactory;

    constructor(context: CacheContext) {
        this.context = context;
        this.conditions = new ConditionBuilder(context);
        this.setItems = new SetItemsFactory(context);
        this.deltaSetItems = new DeltaSetItemsFactory(context);
    }

    createTrigger(): ICacheTrigger {
        return {
            trigger: this.createDatabaseTrigger(),
            function: this.createDatabaseFunction()
        };
    }

    createHelperTrigger(): ICacheTrigger | undefined {
        return;
    }

    private createDatabaseFunction() {

        const func = new DatabaseFunction({
            schema: "public",
            name: this.generateTriggerName(),
            body: "\n" + this.createBody().toSQL() + "\n",
            comment: Comment.fromFs({
                objectType: "function",
                cacheSignature: this.context.cache.getSignature()
            }),
            args: [],
            returns: {type: "trigger"}
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
            insert: this.context.withoutInsertCase() ? false : true,
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
            comment: Comment.fromFs({
                objectType: "trigger",
                cacheSignature: this.context.cache.getSignature()
            })
        });

        return trigger;
    }

    protected replaceTriggerTableToRow(
        row: string,
        expression: Expression
    ) {
        return expression.replaceTable(
            this.context.triggerTable,
            new TableReference(
                this.context.triggerTable,
                row
            )
        );
    }

    protected generateTriggerName() {
        return this.context.generateTriggerName();
    }

    protected abstract createBody(): AbstractAstElement;
}