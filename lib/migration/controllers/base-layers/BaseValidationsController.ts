import {BaseController} from "./BaseController";
import {InputError} from "../../MigrationModel";
import {NamedDBObjectModel} from "../../../objects/NamedDBObjectModel";
import {MaxObjectNameSizeErrorModel} from "../../errors/MaxObjectNameSizeErrorModel";

export 
abstract class BaseValidationsController
extends BaseController {
    protected validateNameLength(dbo: NamedDBObjectModel<any>) {
        if ( dbo.isValidNameLength() ) {
            return;
        }

        const objectType = (
            dbo.constructor.name
                .replace(/Model$/, "")
                .toLowerCase()
        );
        
        const error = new MaxObjectNameSizeErrorModel({
            filePath: dbo.get("filePath"),
            objectType,
            name: dbo.get("name")
        });
        this.throwErrorModel(error);
    }

    protected isValidationError(error: Error) {
        return error.message === "validation_error";
    }

    protected throwErrorModel(error: InputError) {
        this.migration.addError(error);
        throw new Error("validation_error");
    }
}