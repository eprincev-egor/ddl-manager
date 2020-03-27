import { BaseValidator } from "./BaseValidator";
import { NamedDBObjectModel } from "../../../objects/base-layers/NamedDBObjectModel";
import { MaxObjectNameSizeErrorModel } from "../../errors/MaxObjectNameSizeErrorModel";

export class NameValidator extends BaseValidator {
    validate(dbo: NamedDBObjectModel<any>): MaxObjectNameSizeErrorModel {
        
        if ( dbo.isValidNameLength() ) {
            return;
        }

        const errorModel = new MaxObjectNameSizeErrorModel({
            filePath: dbo.get("filePath"),
            objectType: dbo.getTypeName(),
            name: dbo.get("name")
        });

        return errorModel;
    }
}