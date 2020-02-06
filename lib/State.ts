import {Model, Types} from "model-layer";
import {FunctionsCollection} from "./Functions";
import {TriggersCollection} from "./Triggers";
import {ViewsCollection} from "./Views";
import Migration from "./migration/Migration";
import CommandModel from "./migration/commands/CommandModel";
import CreateFunctionCommandModel from "./migration/commands/CreateFunctionCommandModel";

export default class State extends Model<State> {
    structure() {
        return {
            functions: Types.Collection({
                Collection: FunctionsCollection,
                default: new FunctionsCollection()
            }),
            triggers: Types.Collection({
                Collection: TriggersCollection,
                default: new TriggersCollection()
            }),
            views: Types.Collection({
                Collection: ViewsCollection,
                default: new ViewsCollection()
            })
        };
    }

    generateMigration(dbState: State): Migration {
        const fsState: State = this;
        const fsFunctions = fsState.get("functions");
        const dbFunctions = dbState.get("functions");
        const commands: CommandModel[] = [];

        // find functions for create
        fsFunctions.each((fsFunctionModel) => {
            const fsFuncIdentify = fsFunctionModel.getIdentify();
            const dbFunctionModel = dbFunctions.getFunctionByIdentify(fsFuncIdentify);

            if ( dbFunctionModel ) {
                return;
            }

            const command = new CreateFunctionCommandModel({
                function: fsFunctionModel
            });
            commands.push( command );
        });

        return new Migration({
            commands
        });
    }
}