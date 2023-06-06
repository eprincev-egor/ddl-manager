import { AbstractTriggerBuilder, ICacheTrigger } from "./AbstractTriggerBuilder";
import { buildSelfUpdateBySelfRowBody } from "./body/buildSelfUpdateBySelfRowBody";
import { buildSelfAssignBeforeInsertSelfColumnBody } from "./body/buildSelfAssignBeforeInsertSelfColumnBody";

export class SelfUpdateBySelfRowTriggerBuilder 
extends AbstractTriggerBuilder {

    createTriggers(): ICacheTrigger[] {
        return [
            ...this.createOnUpdateTriggers(),
            ...this.createOnInsertTriggers()
        ];
    }

    private createOnUpdateTriggers() {
        const updateOfColumns = this.buildUpdateOfColumns();
        if ( updateOfColumns.length === 0 ) {
            return []
        }

        const selects = this.context.groupBySelectsForUpdateByLevel();
        return selects.map(select => {
            const triggerName = this.context.generateOrderedTriggerName(
                select.columns, "bef_upd"
            );

            return {
                trigger: this.createDatabaseTrigger({
                    name: triggerName,
                    after: false,
                    before: true,
                    insert: false,
                    delete: false,
                }),
                procedure: this.createDatabaseFunction(
                    buildSelfUpdateBySelfRowBody(
                        this.conditions.noChanges(),
                        this.buildSelectValues()
                    ),
                    triggerName
                )
            }
        });
    }

    private createOnInsertTriggers(): ICacheTrigger[] {
        const selects = this.context.groupBySelectsForUpdateByLevel();
        return selects.map(select => {
            const triggerName = this.context.generateOrderedTriggerName(
                select.columns, "bef_ins"
            );

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
                        select.columns
                    ),
                    triggerName
                )
            }
        });
    }

    private buildSelectValues() {
        const columns = this.context.createSelectForUpdateNewRow().columns;
        return columns.sort((columnA, columnB) =>
            this.context.getDependencyIndex(columnA.name) -
            this.context.getDependencyIndex(columnB.name)
        );
    }
}
