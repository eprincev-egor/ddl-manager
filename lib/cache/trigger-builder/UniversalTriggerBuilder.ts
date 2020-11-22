import { buildUniversalBody } from "../processor/buildUniversalBody";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";
import { buildFromAndWhere } from "../processor/buildFromAndWhere";

export class UniversalTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {

        const {from, where} = buildFromAndWhere(
            this.cache,
            this.triggerTable
        );

        const universalBody = buildUniversalBody({
            triggerTable: this.triggerTable,
            forTable: this.cache.for.toString(),
            updateColumns: this.cache.select.columns
                .map(col => col.name),
            select: this.cache.select.toString(),

            from,
            where,
            triggerTableColumns: this.triggerTableColumns
        });
        return universalBody;
    }
}