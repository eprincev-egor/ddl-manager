import BaseController from "./BaseController";

import NamedDBObjectModel from "../../../objects/NamedDBObjectModel";
import MaxObjectNameSizeErrorModel from "../../errors/MaxObjectNameSizeErrorModel";

export default 
abstract class BaseValidationsController
extends BaseController {
    validateNameLength(dbo: NamedDBObjectModel<any>) {
        if ( dbo.isValidNameLength() ) {
            return;
        }

        const objectType = (
            dbo.constructor.name
                .replace(/Model$/, "")
                .toLowerCase()
        );
        return new MaxObjectNameSizeErrorModel({
            filePath: dbo.get("filePath"),
            objectType,
            name: dbo.get("name")
        });
    }
}