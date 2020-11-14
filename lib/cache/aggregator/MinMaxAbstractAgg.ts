import { AbstractAgg } from "./AbstractAgg";

export abstract class MinMaxAbstractAgg extends AbstractAgg {

    protected printSelect(spaces: string) {
        const recalculateSelect = this.select.cloneWith({
            columns: [this.updateColumn]
        }).toString();

        const selectSQL = spaces + recalculateSelect.replace(/\n/g, `\n${spaces}`);
        return selectSQL;
    }
}