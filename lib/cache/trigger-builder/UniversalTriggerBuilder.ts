import { buildUniversalBody } from "./body/buildUniversalBody";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildFrom } from "../processor/buildFrom";
import { buildUniversalWhere } from "../processor/buildUniversalWhere";

export class UniversalTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {

        const from = buildFrom(this.context);
        const where = buildUniversalWhere(this.context);

        const universalBody = buildUniversalBody({
            triggerTable: this.context.triggerTable,
            forTable: this.context.cache.for.toString(),
            updateColumns: this.context.cache.select.columns
                .map(col => col.name),
            select: this.context.cache.select.toString(),

            from,
            where,
            triggerTableColumns: this.context.triggerTableColumns
        });
        return universalBody;
    }
}