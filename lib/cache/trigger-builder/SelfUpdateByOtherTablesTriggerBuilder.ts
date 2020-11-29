import {
    TableReference,
    Expression
} from "../../ast";
import { DatabaseTrigger } from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildSelfUpdateByOtherTablesBody } from "./body/buildSelfUpdateByOtherTablesBody";

export class SelfUpdateByOtherTablesTriggerBuilder extends AbstractTriggerBuilder {

    createBody() {
        let hasReference = this.conditionBuilder.getHasNoReference("new") as Expression;
        hasReference = hasReference.replaceTable(
            this.context.cache.for,
            new TableReference(
                this.context.triggerTable,
                "new"
            )
        );

        // TODO: update also helper columns

        return buildSelfUpdateByOtherTablesBody(
            this.context.cache.for,
            this.conditionBuilder.getNoReferenceChanges(),
            hasReference,
            this.context.cache.select.columns.map(col => col.name),
            this.context.cache.select.toString()
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
            table: {
                schema: this.context.triggerTable.schema || "public",
                name: this.context.triggerTable.name
            },
            cacheSignature: this.context.cache.getSignature()
        });

        return trigger;
    }

}