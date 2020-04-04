import {BaseDBObjectModel} from "../../objects/base-layers/BaseDBObjectModel";
import {FunctionModel} from "../../objects/FunctionModel";
import {TriggerModel} from "../../objects/TriggerModel";
import {ViewModel} from "../../objects/ViewModel";
import {TableModel} from "../../objects/TableModel";
import {ExtensionModel} from "../../objects/ExtensionModel";
import {Parser} from "../Parser";
import {prepareAbstractTable} from "./prepareAbstractTable";
import {
    GrapeQLCoach,
    CreateFunction,
    CreateTrigger,
    CreateView,
    CreateTable,
    Extension
} from "grapeql-lang";

export class PgParser extends Parser {
    parseFile(filePath: string, fileContent: string): BaseDBObjectModel<any>[] {
        const coach = new GrapeQLCoach(fileContent);
        const objects: BaseDBObjectModel<any>[] = [];

        for (; coach.i < coach.str.length; coach.i++) {
            // need ignore comments (inside comment can be invalid syntax)
            coach.skipSpace();

            // create or replace function
            if ( coach.is(CreateFunction) ) {
                const parsedFunction = coach.parse(CreateFunction);
                const {schema, name, args} = parsedFunction.row;
                
                const argsTypes = args.map(arg => 
                    arg.get("type")
                );
                const functionIdentify = `${schema}.${name}(${argsTypes})`;

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

            // create table
            if ( coach.is(CreateTable) ) {
                const parsedTable: CreateTable = coach.parse(CreateTable);
                const {name: tableName, schema} = parsedTable.row;
                const tableIdentify = (schema || "public").toString() + "." + tableName.toString();
                const tableBody = prepareAbstractTable(
                    filePath,
                    tableName.toString(),
                    parsedTable
                );
                
                const tableModel = new TableModel({
                    ...tableBody,
                    filePath,
                    identify: tableIdentify,
                    name: tableName.toString(),
                    parsed: parsedTable
                });

                objects.push(tableModel);
            }

            // extension
            if ( coach.is(Extension) ) {
                const parsedExtension: Extension = coach.parse(Extension);
                const {name, forTable} = parsedExtension.row;
                const forTableIdentify = (
                    forTable.row.link.length === 1 ?
                        "public." + forTable.row.link[0].toString() :
                        forTable.toString()
                );
                const tableBody = prepareAbstractTable(
                    filePath,
                    forTable.toString(),
                    parsedExtension
                );
                
                const extensionModel = new ExtensionModel({
                    ...tableBody,
                    filePath,
                    identify: `extension ${name} for ${forTableIdentify}`,
                    name: name.toString(),
                    forTableIdentify,
                    parsed: parsedExtension
                });

                objects.push(extensionModel);
            }
        }

        return objects;
    }
}