import {SimpleMigrator} from "./base-layers/SimpleMigrator";
import { NameValidator } from "./validators/NameValidator";
import { ViewModel } from "../../objects/ViewModel";
import { ViewCommandModel } from "../commands/ViewCommandModel";

export class ViewsMigrator
extends SimpleMigrator<ViewModel> {

    protected calcChanges() {
        const fsViews = this.fs.row.views;
        const dbViews = this.db.row.views;
        const changes = fsViews.compareWithDB(dbViews);
        return changes;
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