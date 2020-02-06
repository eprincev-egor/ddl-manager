import {Collection, Model, Types} from "model-layer";

export class ViewModel extends Model<ViewModel> {
    structure() {
        return {
            schema: Types.String,
            name: Types.String
        };
    }

    getIdentify() {
        return this.row.schema + "." + this.row.name;
    }
}

export class ViewsCollection extends Collection<ViewModel> {
    Model() {return ViewModel;}

    getViewByIdentify(viewIdentify: string): ViewModel {
        const existsViewModel = this.find(viewModel =>
            viewModel.getIdentify() === viewIdentify
        );

        return existsViewModel;
    }
}