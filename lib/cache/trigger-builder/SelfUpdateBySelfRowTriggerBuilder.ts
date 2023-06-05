import { TableReference } from "../../database/schema/TableReference";
import { AbstractTriggerBuilder, ICacheTrigger } from "./AbstractTriggerBuilder";
import { buildSelfUpdateBySelfRowBody } from "./body/buildSelfUpdateBySelfRowBody";
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
                after: false,
                before: true,
                insert: false,
                delete: false,
            }),
            procedure: this.createDatabaseFunction(
                buildSelfUpdateBySelfRowBody(
                    this.conditions.noChanges(),
                    this.buildSelectValues()
                )
            )
        }, ...this.createBeforeInsertTriggers()];
    }

    private buildSelectValues() {
        const columns = this.context.createSelectForUpdateNewRow().columns;
        return columns.sort((columnA, columnB) =>
            this.context.getDependencyIndex(columnA.name) -
            this.context.getDependencyIndex(columnB.name)
        );
    }

    private createBeforeInsertTriggers(): ICacheTrigger[] {
        const selectValues = this.context.createSelectForUpdateNewRow();

        return selectValues.columns.map(selectColumn => {
            const dependencyIndex = this.context.getDependencyIndex(
                selectColumn.name
            );
            const triggerName = [
                `cache${leadingZero(dependencyIndex, 3)}`,
                this.context.cache.for.table.name,
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
