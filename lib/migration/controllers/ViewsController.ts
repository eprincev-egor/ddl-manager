import DefaultStrategyController from "./base-layers/DefaultStrategyController";
import ViewCommandModel from "../commands/ViewCommandModel";
import ViewModel from "../../objects/ViewModel";

export default 
class ViewsController 
extends DefaultStrategyController<ViewModel> {
    
    detectChanges() {
        const dbViews = this.db.row.views;
        const fsViews = this.fs.row.views;
        return fsViews.compareWithDB(dbViews);
    }

    validate(viewModel: ViewModel) {
        return [
            this.validateNameLength(viewModel)
        ];
    }

    getDropCommand(viewModel: ViewModel) {
        return new ViewCommandModel({
            type: "drop",
            view: viewModel
        });
    }

    getCreateCommand(viewModel: ViewModel) {
        return new ViewCommandModel({
            type: "create",
            view: viewModel
        });
    }
}