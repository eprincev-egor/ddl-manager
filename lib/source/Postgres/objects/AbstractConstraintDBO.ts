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
    
    getIdentify() {
        return `constraint ${this.row.name} on ${this.row.table}`;
    }

    equal(other: this) {
        return super.equal(other);
    }
    
    toDropSQL() {
        const row = this.row;
        return `
            alter table ${row.table}
            drop constraint ${row.name}
        `;
    }
}