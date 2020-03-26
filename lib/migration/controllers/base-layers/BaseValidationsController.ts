import {BaseController} from "./BaseController";
import {InputError} from "../../MigrationModel";
import {NamedDBObjectModel} from "../../../objects/base-layers/NamedDBObjectModel";
import {MaxObjectNameSizeErrorModel} from "../../errors/MaxObjectNameSizeErrorModel";

export 
abstract class BaseValidationsController
extends BaseController {
    protected validateNameLength(dbo: NamedDBObjectModel<any>) {
        if ( dbo.isValidNameLength() ) {
            return;
        }

        const error = this.createInvalidNameError(dbo);
        this.throwErrorModel(error);
    }

    protected createInvalidNameError(dbo: NamedDBObjectModel<any>) {
        const error = new MaxObjectNameSizeErrorModel({
            filePath: dbo.get("filePath"),
            objectType: getObjectTypeFromConstructorName(dbo),
            name: dbo.get("name")
        });
        return error;
    }

    protected isValidationError(error: Error) {
        return error.message === "validation_error";
    }

    protected throwErrorModel(error: InputError) {
        this.migration.addError(error);
        throw new Error("validation_error");
    }
}

function getObjectTypeFromConstructorName(dbo: NamedDBObjectModel<any>): string {
    return dbo.constructor.name
        .replace(/Model$/, "")
        .toLowerCase()
}