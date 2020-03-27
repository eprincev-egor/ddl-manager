import { BaseValidator } from "./BaseValidator";
import { ForeignKeyConstraintModel } from "../../../objects/ForeignKeyConstraintModel";
import { ReferenceToUnknownTableErrorModel } from "../../errors/ReferenceToUnknownTableErrorModel";
import { ReferenceToUnknownColumnErrorModel } from "../../errors/ReferenceToUnknownColumnErrorModel";
import { TableModel } from "../../../objects/TableModel";

export class ForeignKeyValidator extends BaseValidator {
    validate(): any {
        return null;
    }

    validateTable(table: TableModel, fk: ForeignKeyConstraintModel) {
        const errorModel = (
            this.validateReferenceTable(table, fk) ||
            this.validateReferenceColumns(table, fk)
        );
        return errorModel;
    }

    private validateReferenceTable(
        table: TableModel, 
        fk: ForeignKeyConstraintModel
    ): ReferenceToUnknownTableErrorModel {
        const referenceTableIdentify = fk.get("referenceTableIdentify");
        const referenceTableModel = this.db.row.tables.getByIdentify(referenceTableIdentify);

        if ( !referenceTableModel ) {
            const errorModel = new ReferenceToUnknownTableErrorModel({
                filePath: table.get("filePath"),
                foreignKeyName: fk.get("name"),
                tableIdentify: table.getIdentify(),
                referenceTableIdentify
            });
            return errorModel;
        }
    }

    private validateReferenceColumns(
        table: TableModel, 
        fk: ForeignKeyConstraintModel
    ): ReferenceToUnknownColumnErrorModel {
        const referenceTableIdentify = fk.get("referenceTableIdentify");
        const referenceTableModel = this.db.row.tables.getByIdentify(referenceTableIdentify);

        const referenceColumns = fk.get("referenceColumns");
        const unknownColumns = [];
        referenceColumns.forEach(key => {
            const existsColumn = referenceTableModel.get("columns").find(column =>
                column.get("key") === key
            );

            if ( !existsColumn ) {
                unknownColumns.push(key);
            }
        });

        if ( unknownColumns.length ) {
            const errorModel = new ReferenceToUnknownColumnErrorModel({
                filePath: table.get("filePath"),
                foreignKeyName: fk.get("name"),
                tableIdentify: table.getIdentify(),
                referenceTableIdentify,
                referenceColumns: unknownColumns
            });

            return errorModel;
        }
    }
}