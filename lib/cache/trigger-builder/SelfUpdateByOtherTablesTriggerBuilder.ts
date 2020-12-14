import { Expression } from "../../ast";
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

        const selectToUpdate = createSelectForUpdate(
            this.context.database,
            this.context.cache
        );

        return buildSelfUpdateByOtherTablesBody(
            this.context.cache.for,
            this.conditions.noReferenceChanges(),
            hasReference,
            selectToUpdate.columns.map(col => col.name),
            selectToUpdate.toString()
        );
    }


    protected createDatabaseTrigger() {
        
        const updateOfColumns = this.context.triggerTableColumns
            .filter(column =>  column !== "id" )
            .sort();
        
        const trigger = new DatabaseTrigger({
            name: this.generateTriggerName(),
            after: true,
            insert: true,
            
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