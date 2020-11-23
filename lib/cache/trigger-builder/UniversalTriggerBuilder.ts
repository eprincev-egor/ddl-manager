import { buildUniversalBody } from "../processor/buildUniversalBody";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildFromAndWhere } from "../processor/buildFromAndWhere";

export class UniversalTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {

        const {from, where} = buildFromAndWhere(
            this.context.cache,
            this.context.triggerTable
        );

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