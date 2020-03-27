import {SimpleMigrator} from "./base-layers/SimpleMigrator";
import { NameValidator } from "./validators/NameValidator";
import { ViewModel } from "../../objects/ViewModel";
import { ViewCommandModel } from "../commands/ViewCommandModel";

export class ViewsMigrator
extends SimpleMigrator<ViewModel> {

    protected calcChanges() {
        return this.fs.compareViewsWithDB(this.db);
    }

    protected getValidators() {
        return [
            NameValidator
        ];
    }

    protected createDropCommand(viewModel: ViewModel) {
        return new ViewCommandModel({
            type: "drop",
            view: viewModel
        });
    }

    protected createCreateCommand(viewModel: ViewModel) {
        return new ViewCommandModel({
            type: "create",
            view: viewModel
        });
    }

}