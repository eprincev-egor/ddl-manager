import { flatMap, uniq } from "lodash";
import {
    AbstractAstElement,
    Expression
} from "../../ast";
import { Comment } from "../../database/schema/Comment";
import { DatabaseFunction } from "../../database/schema/DatabaseFunction";
import { DatabaseTrigger, IDatabaseTriggerParams } from "../../database/schema/DatabaseTrigger";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { CacheContext } from "./CacheContext";
import { ConditionBuilder } from "./condition/ConditionBuilder";
import { DEFAULT_SCHEMA } from "../../parser/defaults";

export interface ICacheTrigger {
    trigger: DatabaseTrigger;
    procedure: DatabaseFunction;
}

export abstract class AbstractTriggerBuilder {

    protected readonly context: CacheContext;
    protected readonly conditions: ConditionBuilder;

    constructor(context: CacheContext) {
        this.context = context;
        this.conditions = new ConditionBuilder(context);
    }

    abstract createTriggers(): ICacheTrigger[];

    protected createDatabaseTriggerOnDIU() {
        const updateOfColumns = this.buildUpdateOfColumns();

        return this.createDatabaseTrigger({
            name: this.context.generateTriggerName(),

            after: true,
            insert: this.needListenInsert(),
            update: updateOfColumns.length > 0,
            updateOf: updateOfColumns,
            delete: true,
        });
    }

    protected createDatabaseTrigger(
        json: Partial<IDatabaseTriggerParams> = {}
    ) {
        const triggerName = json.name || this.context.generateTriggerName();

        const trigger = new DatabaseTrigger({
            name: triggerName,

            before: false,
            after: false,
            insert: false,
            update: false,
            delete: false,

            procedure: {
                schema: "public",
                name: triggerName,
                args: []
            },
            table: new TableID(
                this.context.triggerTable.schema || DEFAULT_SCHEMA,
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
        name = this.context.generateTriggerName()
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

    protected buildUpdateOfColumns() {
        const updateOfColumns = this.context.triggerTableColumns
            .filter(column =>  column !== "id" );

        this.addCustomBeforeUpdateTriggerDeps(updateOfColumns);
        this.addCacheBeforeUpdateTriggerDeps(updateOfColumns);

        return uniq(updateOfColumns).sort();
    }

    private addCustomBeforeUpdateTriggerDeps(updateOfColumns: string[]) {
        const beforeUpdateTriggers = this.context.getBeforeUpdateTriggers();

        for (const beforeUpdateTrigger of beforeUpdateTriggers) {
            const dbFunction = this.context.getTriggerFunction(beforeUpdateTrigger);

            const changedColumns = dbFunction.findAssignColumns() || [];
            const hasDepsColumns = changedColumns.some(columnName =>
                updateOfColumns.includes(columnName)
            );

            if ( hasDepsColumns && beforeUpdateTrigger.updateOf ) {
                updateOfColumns.push(...beforeUpdateTrigger.updateOf);
            }
        }
    }

    private addCacheBeforeUpdateTriggerDeps(updateOfColumns: string[]) {
        const depsCaches = this.context.allCacheForTriggerTable.filter(cache =>
            cache.select.columns.some(column => 
                updateOfColumns.includes(column.name)
            )
        );

        const depsColumns = flatMap(depsCaches, cache =>
            cache.getTargetTablesDepsColumns()
        ).filter(column => column != "id");

        const newColumns = depsColumns.filter(depColumn =>
            !updateOfColumns.includes(depColumn)
        );
        if ( newColumns.length ) {
            updateOfColumns.push( ...depsColumns );

            this.addCacheBeforeUpdateTriggerDeps(updateOfColumns)
        }
    }
}