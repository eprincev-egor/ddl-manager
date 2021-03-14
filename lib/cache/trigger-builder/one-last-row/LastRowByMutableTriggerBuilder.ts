import {
    Update, Expression, SetItem,
    SelectColumn, UnknownExpressionElement, 
    SimpleSelect,
    Body
} from "../../../ast";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByIdBody } from "../body/buildOneLastRowByIdBody";

export class LastRowByMutableTriggerBuilder extends AbstractLastRowTriggerBuilder {

    protected createBody() {
        const body = new Body({
            declares: [],
            statements: []
        });
        return body;
    }

}