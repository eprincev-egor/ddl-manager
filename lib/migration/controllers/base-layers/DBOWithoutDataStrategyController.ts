import {BaseStrategyController} from "./BaseStrategyController";
import {NamedAndMovableDBOModel} from "../../../objects/base-layers/NamedAndMovableDBOModel";

export 
abstract class DBOWithoutDataStrategyController<DBOModel extends NamedAndMovableDBOModel<any>>
extends BaseStrategyController<DBOModel> {

    onRemove(dbo: DBOModel) {
        if ( dbo.allowedToDrop() ) {
            this.drop(dbo);
        }
    }

}