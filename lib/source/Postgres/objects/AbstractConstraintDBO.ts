import { IDBO } from "../../../common";
import { Model, Types } from "model-layer";

export abstract class AbstractConstraintDBO<ChildConstraintDBO extends AbstractConstraintDBO<any>> 
extends Model<ChildConstraintDBO>
implements IDBO {
    structure() {
        return {
            table: Types.String({
                required: true
            }),
            name: Types.String({
                required: true
            })
        };
    }

    abstract toCreateSQL(): string;

    toDropSQL() {
        const row = this.row;
        return `
            alter table ${row.table}
            drop constraint ${row.name}
        `;
    }
}