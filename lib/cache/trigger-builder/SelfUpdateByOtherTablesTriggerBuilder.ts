import { Expression, NotExpression } from "../../ast";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { Comment } from "../../database/schema/Comment";
import { DatabaseTrigger } from "../../database/schema/DatabaseTrigger";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildSelfUpdateByOtherTablesBody } from "./body/buildSelfUpdateByOtherTablesBody";
import { createSelectForUpdate } from "../processor/createSelectForUpdate";

export class SelfUpdateByOtherTablesTriggerBuilder extends AbstractTriggerBuilder {

    createBody() {
        let hasReference = this.conditions.hasNoReference("new") as Expression;
        hasReference = hasReference.replaceTable(
            this.context.cache.for,
            new TableReference(
                this.context.triggerTable,
                "new"
            )
        );

        let notMatchedFilterOnInsert: Expression | undefined;
        let notMatchedFilterOnUpdate: Expression | undefined;

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

            notMatchedFilterOnInsert = Expression.or(notMatchedNew);
            notMatchedFilterOnUpdate = Expression.and([
                Expression.or(notMatchedOld),
                Expression.or(notMatchedNew)
            ])
        }

        const selectToUpdate = createSelectForUpdate(
            this.context.database,
            this.context.cache
        );

        return buildSelfUpdateByOtherTablesBody(
            this.context.withoutInsertCase() ? false : true,
            this.context.cache.for,
            this.conditions.noReferenceChanges(),
            hasReference,
            selectToUpdate.columns.map(col => col.name),
            selectToUpdate.toString(),
            notMatchedFilterOnInsert,
            notMatchedFilterOnUpdate
        );
    }


    protected createDatabaseTrigger() {
        
        const updateOfColumns = this.context.triggerTableColumns
            .filter(column =>  column !== "id" )
            .sort();
        
        const trigger = new DatabaseTrigger({
            name: this.generateTriggerName(),
            after: true,
            insert: this.context.withoutInsertCase() ? false : true,
            
            delete: false,

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

}