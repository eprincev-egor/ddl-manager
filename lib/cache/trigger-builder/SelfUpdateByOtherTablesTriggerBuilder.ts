import { Expression, NotExpression } from "../../ast";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { Comment } from "../../database/schema/Comment";
import { DatabaseTrigger } from "../../database/schema/DatabaseTrigger";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildSelfUpdateByOtherTablesBody } from "./body/buildSelfUpdateByOtherTablesBody";
import { buildSelfAssignBeforeInsertByOtherTablesBody } from "./body/buildSelfAssignBeforeInsertByOtherTablesBody";
import { createSelectForUpdate } from "../processor/createSelectForUpdate";
import { DatabaseFunction } from "../../database/schema/DatabaseFunction";
import { leadingZero } from "./utils";
import { groupBy, uniq } from "lodash";

export class SelfUpdateByOtherTablesTriggerBuilder extends AbstractTriggerBuilder {

    createTriggers() {
        const updateOfColumns = this.buildUpdateOfColumns();
        if ( updateOfColumns.length === 0 ) {
            return this.createHelperTriggers();
        }

        return [{
            trigger: this.createDatabaseTrigger({
                after: true,
                insert: false,
                delete: false,
            }),
            procedure: this.createDatabaseFunction(
                this.createBody()
            )
        }, ...this.createHelperTriggers()];
    }

    private createHelperTriggers() {
        if ( !this.needInsertCase() ) {
            return [];
        }

        const {notMatchedFilterOnInsert} = this.buildNotMatchedConditions();
        
        const {cache} = this.context;
        const newRow = new TableReference(
            cache.for.table,
            "new"
        );

        const selectValues = createSelectForUpdate(
            this.context.database,
            this.context.cache
        ).replaceTable(cache.for, newRow);

        const columnsByLevel = groupBy(selectValues.columns, column => 
            this.context.getDependencyLevel(column.name)
        );

        return Object.values(columnsByLevel).map(columns => {
            const dependencyIndexes = columns.map(column =>
                this.context.getDependencyIndex(column.name)
            );
            const minDependencyIndex = Math.min(...dependencyIndexes);

            const triggerName = [
                `cache${leadingZero(minDependencyIndex, 3)}`,
                cache.name,
                "for",
                cache.for.table.name,
                "bef_ins"
            ].join("_");


            return {
                trigger: new DatabaseTrigger({
                    name: triggerName,
                    before: true,
                    insert: true,
        
                    procedure: {
                        schema: "public",
                        name: triggerName,
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
                }),
                procedure: new DatabaseFunction({
                    schema: "public",
                    name: triggerName,
                    body: "\n" + buildSelfAssignBeforeInsertByOtherTablesBody(
                        selectValues.cloneWith({
                            columns
                        }),
                        notMatchedFilterOnInsert
                    ).toSQL() + "\n",
                    comment: Comment.fromFs({
                        objectType: "function",
                        cacheSignature: this.context.cache.getSignature()
                    }),
                    args: [],
                    returns: {type: "trigger"}
                })
            }
        })
    }

    private createBody() {
        let hasReference = this.conditions.hasNoReference("new") as Expression;
        hasReference = hasReference.replaceTable(
            this.context.cache.for,
            new TableReference(
                this.context.triggerTable,
                "new"
            )
        );

        const {notMatchedFilterOnUpdate} = this.buildNotMatchedConditions();

        const selectToUpdate = createSelectForUpdate(
            this.context.database,
            this.context.cache
        );

        return buildSelfUpdateByOtherTablesBody(
            this.context.cache.for,
            this.conditions.noChanges(),
            selectToUpdate.columns.map(col => col.name),
            selectToUpdate.toString(),
            notMatchedFilterOnUpdate
        );
    }

    protected buildNotMatchedConditions() {
        if ( this.context.referenceMeta.cacheTableFilters.length ) {
            const notMatchedNew: NotExpression[] = [];
            const notMatchedOld: NotExpression[] = [];
            for (const filter of this.context.referenceMeta.cacheTableFilters) {
                const notFilterNew = new NotExpression(
                    this.replaceTriggerTableToRow("new", filter)
                );
                const notFilterOld = new NotExpression(
                    this.replaceTriggerTableToRow("old", filter)
                );
                notMatchedNew.push(notFilterNew);
                notMatchedOld.push(notFilterOld);
            }

            return { 
                notMatchedFilterOnInsert: Expression.or(notMatchedNew),
                notMatchedFilterOnUpdate:  Expression.and([
                    Expression.or(notMatchedOld),
                    Expression.or(notMatchedNew)
                ])
            }
        }

        return { 
            notMatchedFilterOnInsert: undefined,
            notMatchedFilterOnUpdate: undefined
        }
    }

    private needInsertCase(): boolean {
        if ( this.context.withoutInsertCase() ) {
            return false;
        }

        const hasReference = this.conditions.hasNoReference("new");
        if ( !hasReference || hasReference.isEmpty() ) {
            return false;
        }

        return true;
    }
}