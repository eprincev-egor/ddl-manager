import { buildUniversalBody } from "../processor/buildUniversalBody";
import { AbstractTriggerBuilder } from "./AbstractTriggerBuilder";


export class UniversalTriggerBuilder extends AbstractTriggerBuilder {

    protected createBody() {
        const universalBody = buildUniversalBody({
            triggerTable: this.triggerTable,
            forTable: this.cache.for.toString(),
            updateColumns: this.cache.select.columns
                .map(col => col.name),
            select: this.cache.select.toString(),

            from: this.from,
            where: this.where,
            triggerTableColumns: this.triggerTableColumns
        });
        return universalBody;
    }
}