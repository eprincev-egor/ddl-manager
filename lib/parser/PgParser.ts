import BaseDBObjectModel from "../objects/BaseDBObjectModel";
import FunctionModel from "../objects/FunctionModel";
import Parser from "./Parser";
import {
    GrapeQLCoach,
    CreateFunction,
    CreateTrigger,
    CreateView
} from "grapeql-lang";
import TriggerModel from "../objects/TriggerModel";
import ViewModel from "../objects/ViewModel";

export default class PgParser extends Parser {
    parseFile(filePath: string, fileContent: string): BaseDBObjectModel<any>[] {
        const coach = new GrapeQLCoach(fileContent);
        const objects: BaseDBObjectModel<any>[] = [];

        for (; coach.i < coach.str.length; coach.i++) {
            // create or replace function
            if ( coach.is(CreateFunction) ) {
                const parsedFunction = coach.parse(CreateFunction);
                const {schema, name, args} = parsedFunction.row;
                const functionIdentify = `${schema}.${name}(${args.join(",")})`;

                const funcModel = new FunctionModel({
                    filePath,
                    identify: functionIdentify,
                    name: parsedFunction.get("name"),
                    parsed: parsedFunction
                });
                
                objects.push(funcModel);
            }

            // create trigger
            if ( coach.is(CreateTrigger) ) {
                const parsedTrigger = coach.parse(CreateTrigger);
                const {name, table, procedure} = parsedTrigger.row;
                const triggerIdentify = name + " on " + table.toString();
                
                const triggerModel = new TriggerModel({
                    filePath,
                    name,
                    identify: triggerIdentify,
                    parsed: parsedTrigger,
                    tableIdentify: table.toString(),
                    functionIdentify: procedure.toString()
                });

                objects.push(triggerModel);
            }

            // create view
            if ( coach.is(CreateView) ) {
                const parsedView = coach.parse(CreateView);
                const {schema, name} = parsedView.row;
                const viewIdentify = (schema || "public").toString() + "." + name.toString();
                
                const viewModel = new ViewModel({
                    filePath,
                    name: name.toString(),
                    identify: viewIdentify,
                    parsed: parsedView
                });

                objects.push(viewModel);
            }
        }

        return objects;
    }
}