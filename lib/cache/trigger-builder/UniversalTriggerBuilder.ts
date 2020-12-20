import { buildUniversalBody } from "./body/buildUniversalBody";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildFrom } from "../processor/buildFrom";
import { buildUniversalWhere } from "../processor/buildUniversalWhere";
import { createSelectForUpdate } from "../processor/createSelectForUpdate";

export class UniversalTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {

        const from = buildFrom(this.context);
        const where = buildUniversalWhere(this.context);

        const select = createSelectForUpdate(
            this.context.database,
            this.context.cache
        );

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