import { uniq } from "lodash";
import {
    AbstractAstElement,
    Expression
} from "../../ast";
import { Comment } from "../../database/schema/Comment";
import { DatabaseFunction } from "../../database/schema/DatabaseFunction";
import { DatabaseTrigger, IDatabaseTriggerParams } from "../../database/schema/DatabaseTrigger";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { DeltaSetItemsFactory } from "../aggregator/DeltaSetItemsFactory";
import { SetItemsFactory } from "../aggregator/SetItemsFactory";
import { CacheContext } from "./CacheContext";
import { ConditionBuilder } from "./condition/ConditionBuilder";

export interface ICacheTrigger {
    trigger: DatabaseTrigger;
    procedure: DatabaseFunction;
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

    abstract createTriggers(): ICacheTrigger[];

    protected createDatabaseTrigger(
        json: Partial<IDatabaseTriggerParams> = {}
    ) {
        const updateOfColumns = this.buildUpdateOfColumns();

        const trigger = new DatabaseTrigger({
            name: this.generateTriggerName(),

            after: true,
            insert: this.needListenInsert(),
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
            }),

            ...json
        });
        return trigger;
    }

    protected createDatabaseFunction(
        body: AbstractAstElement,
        name = this.generateTriggerName()
    ) {
        const func = new DatabaseFunction({
            schema: "public",
            name,
            body: "\n" + body.toSQL() + "\n",
            comment: Comment.fromFs({
                objectType: "function",
                cacheSignature: this.context.cache.getSignature()
            }),
            args: [],
            returns: {type: "trigger"}
        });
        return func;
    }

    // can be redefined
    protected needListenInsert(): boolean {
        return !this.context.withoutInsertCase();
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

    protected generateTriggerName(postfix?: string) {
        return this.context.generateTriggerName(postfix);
    }

    protected buildUpdateOfColumns() {

        let updateOfColumns = this.context.triggerTableColumns
            .filter(column =>  column !== "id" );
        
        const triggerDbTable = this.context.database.getTable(
            this.context.triggerTable
        );
        if ( triggerDbTable ) {
            const beforeUpdateTriggers = triggerDbTable.triggers
                .filter(trigger =>
                    trigger.before &&
                    trigger.update
                );

            for (const beforeUpdateTrigger of beforeUpdateTriggers) {
                const dbFunction = this.context.database.functions
                    .find(func =>
                        func.name === beforeUpdateTrigger.procedure.name &&
                        func.schema === beforeUpdateTrigger.procedure.schema
                    );
                if ( dbFunction ) {
                    const matches = dbFunction.body.match(/new\.\w+\s*=/g) || [];
                    const changedColumns = matches.map(str =>
                        str
                            .replace(/^new\./, "")
                            .replace(/\s*=$/, "")
                            .toLowerCase()
                    );
                    const hasDepsColumns = changedColumns.some(columnName =>
                        updateOfColumns.includes(columnName)
                    );

                    if ( hasDepsColumns ) {
                        updateOfColumns = updateOfColumns.concat(
                            (beforeUpdateTrigger.updateOf || [])
                        );
                    }
                }
            }
        }
        updateOfColumns = uniq(updateOfColumns).sort();
        
        return updateOfColumns;
    }
}