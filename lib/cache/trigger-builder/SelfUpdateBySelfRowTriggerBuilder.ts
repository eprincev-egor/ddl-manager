import { TableReference } from "../../database/schema/TableReference";
import { AbstractTriggerBuilder, ICacheTrigger } from "./AbstractTriggerBuilder";
import { buildSelfUpdateBySelfRowBody } from "./body/buildSelfUpdateBySelfRowBody";
import { createSelectForUpdate } from "../processor/createSelectForUpdate";
import { buildSelfAssignBeforeInsertSelfColumnBody } from "./body/buildSelfAssignBeforeInsertSelfColumnBody";
import { leadingZero } from "./utils";

export class SelfUpdateBySelfRowTriggerBuilder 
extends AbstractTriggerBuilder {

    createTriggers(): ICacheTrigger[] {
        const updateOfColumns = this.buildUpdateOfColumns();
        if ( updateOfColumns.length === 0 ) {
            return this.createBeforeInsertTriggers();
        }

        return [{
            trigger: this.createDatabaseTrigger({
                after: true,
                insert: false,
                delete: false,
            }),
            procedure: this.createDatabaseFunction(
                buildSelfUpdateBySelfRowBody(
                    this.context.cache.for,
                    this.conditions.noChanges(),
                    this.buildSelectValues()
                )
            )
        }, ...this.createBeforeInsertTriggers()];
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

    private createBeforeInsertTriggers(): ICacheTrigger[] {
        const {cache} = this.context;
        const newRow = new TableReference(
            cache.for.table,
            "new"
        );

        const selectValues = createSelectForUpdate(
            this.context.database,
            this.context.cache
        ).replaceTable(cache.for, newRow);

        return selectValues.columns.map(selectColumn => {
            const dependencyIndex = this.context.getDependencyIndex(
                selectColumn.name
            );
            const triggerName = [
                `cache${leadingZero(dependencyIndex, 3)}`,
                cache.for.table.name,
                selectColumn.name,
                "bef_ins"
            ].join("_");
    
            return {
                trigger: this.createDatabaseTrigger({
                    after: false,
                    delete: false,
                    update: false,
                    updateOf: undefined,

                    name: triggerName,
                    before: true,
                    insert: true,
                    procedure: {
                        schema: "public",
                        name: triggerName,
                        args: []
                    }
                }),
                procedure: this.createDatabaseFunction(
                    buildSelfAssignBeforeInsertSelfColumnBody(
                        selectColumn
                    ),
                    triggerName
                )
            }
        });
    }
}
