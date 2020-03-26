import {DBOWithoutDataStrategyController} from "./base-layers/DBOWithoutDataStrategyController";
import {ViewCommandModel} from "../commands/ViewCommandModel";
import {ViewModel} from "../../objects/ViewModel";

export 
class ViewsController 
extends DBOWithoutDataStrategyController<ViewModel> {
    
    protected detectChanges() {
        const dbViews = this.db.row.views;
        const fsViews = this.fs.row.views;
        return fsViews.compareWithDB(dbViews);
    }

    protected validate(viewModel: ViewModel) {
        this.validateNameLength(viewModel)
    }

    protected getDropCommand(viewModel: ViewModel) {
        return new ViewCommandModel({
            type: "drop",
            view: viewModel
        });
    }

    protected getCreateCommand(viewModel: ViewModel) {
        return new ViewCommandModel({
            type: "create",
            view: viewModel
        });
    }
}