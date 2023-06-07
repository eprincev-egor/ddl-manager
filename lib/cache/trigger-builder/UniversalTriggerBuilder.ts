import { buildUniversalBody } from "./body/buildUniversalBody";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildFrom } from "../processor/buildFrom";
import { buildUniversalWhere } from "../processor/buildUniversalWhere";

export class UniversalTriggerBuilder extends AbstractTriggerBuilder {

    createTriggers() {
        return [{
            trigger: this.createDatabaseTriggerOnDIU(),
            procedure: this.createDatabaseFunction(
                this.createBody()
            )
        }];
    }

    protected createBody() {

        const from = buildFrom(this.context);
        const where = buildUniversalWhere(this.context);

        const select = this.context.createSelectForUpdate();

        const universalBody = buildUniversalBody({
            triggerTable: this.context.triggerTable,
            forTable: this.context.cache.for.toString(),
            updateColumns: select.columns
                .map(col => col.name),
            select: select.toString(),

            from,
            where,
            triggerTableColumns: this.context.triggerTableColumns
        });
        return universalBody;
    }
}