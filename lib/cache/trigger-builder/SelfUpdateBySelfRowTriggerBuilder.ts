import { DatabaseTrigger, TableReference } from "../../ast";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildSelfUpdateBySelfRowBody } from "./body/buildSelfUpdateBySelfRowBody";

export class SelfUpdateBySelfRowTriggerBuilder extends AbstractTriggerBuilder {

    createBody() {
        return buildSelfUpdateBySelfRowBody(
            this.context.cache.for,
            this.conditionBuilder.getNoChanges(),
            this.buildSelectValues()
        );
    }

    private buildSelectValues() {
        const {cache} = this.context;
        const newRow = new TableReference(
            cache.for.table,
            "new"
        );

        const columns = cache.select.columns.map(column => {
            const newExpression = column.expression.replaceTable(cache.for, newRow);
            const newColumn = column.replaceExpression(newExpression);
            return newColumn;
        });

        const selectValues = cache.select.cloneWith({
            columns
        })
        return selectValues;
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