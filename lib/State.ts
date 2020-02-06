import {Model, Types} from "model-layer";
import FunctionsCollection from "./objects/FunctionsCollection";
import TriggersCollection from "./objects/TriggersCollection";
import ViewsCollection from "./objects/ViewsCollection";
import Migration from "./migration/Migration";
import CommandModel from "./migration/commands/CommandModel";
import FunctionCommandModel from "./migration/commands/FunctionCommandModel";
import ViewCommandModel from "./migration/commands/ViewCommandModel";

export default class State extends Model<State> {
    structure() {
        return {
            functions: Types.Collection({
                Collection: FunctionsCollection,
                default: () => new FunctionsCollection()
            }),
            triggers: Types.Collection({
                Collection: TriggersCollection,
                default: () => new TriggersCollection()
            }),
            views: Types.Collection({
                Collection: ViewsCollection,
                default: () => new ViewsCollection()
            })
        };
    }

    generateMigration(dbState: State): Migration {
        const fsState: State = this;
        const fsFunctions = fsState.get("functions");
        const dbFunctions = dbState.get("functions");
        const fsViews = fsState.get("views");
        const dbViews = dbState.get("views");
        const commands: CommandModel[] = [];

        // drop functions
        dbFunctions.each((dbFunctionModel) => {
            const dbFuncIdentify = dbFunctionModel.getIdentify();
            const fsFunctionModel = fsFunctions.getByIdentify(dbFuncIdentify);

            if ( fsFunctionModel ) {
                return;
            }

            const command = new FunctionCommandModel({
                type: "drop",
                function: dbFunctionModel
            });
            commands.push( command );
        });

        // create functions
        fsFunctions.each((fsFunctionModel) => {
            const fsFuncIdentify = fsFunctionModel.getIdentify();
            const dbFunctionModel = dbFunctions.getByIdentify(fsFuncIdentify);

            if ( dbFunctionModel ) {
                return;
            }

            const command = new FunctionCommandModel({
                type: "create",
                function: fsFunctionModel
            });
            commands.push( command );
        });

        // drop views
        dbViews.each((dbViewModel) => {
            const dbViewIdentify = dbViewModel.getIdentify();
            const fsViewModel = fsViews.getByIdentify(dbViewIdentify);

            if ( fsViewModel ) {
                return;
            }

            const command = new ViewCommandModel({
                type: "drop",
                view: dbViewModel
            });
            commands.push( command );
        });

        // create views
        fsViews.each((fsViewModel) => {
            const fsViewIdentify = fsViewModel.getIdentify();
            const dbViewModel = dbViews.getByIdentify(fsViewIdentify);

            if ( dbViewModel ) {
                return;
            }

            const command = new ViewCommandModel({
                type: "create",
                view: fsViewModel
            });
            commands.push(command);
        });

        // output migration
        return new Migration({
            commands
        });
    }
}