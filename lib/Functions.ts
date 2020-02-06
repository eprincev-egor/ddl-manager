import {Collection, Model, Types} from "model-layer";

export class FunctionModel extends Model<FunctionModel> {
    structure() {
        return {
            schema: Types.String,
            name: Types.String
        };
    }

    getIdentify(): string {
        return this.row.schema + "." + this.row.name;
    }
}

export class FunctionsCollection extends Collection<FunctionModel> {
    Model() {return FunctionModel;}

    getFunctionByIdentify(functionIdentify: string): FunctionModel {
        const existsFunctionModel = this.find(functionModel =>
            functionModel.getIdentify() === functionIdentify
        );

        return existsFunctionModel;
    }
}