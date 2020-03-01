import BaseDBObjectModel from "../objects/BaseDBObjectModel";
import FunctionModel from "../objects/FunctionModel";
import TriggerModel from "../objects/TriggerModel";
import ViewModel from "../objects/ViewModel";
import Parser from "./Parser";
import {
    GrapeQLCoach,
    CreateFunction,
    CreateTrigger,
    CreateView,
    CreateTable,
    PrimaryKeyConstraint,
    UniqueConstraint,
    CheckConstraint
} from "grapeql-lang";
import TableModel from "../objects/TableModel";

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

            // create table
            if ( coach.is(CreateTable) ) {
                const parsedTable: CreateTable = coach.parse(CreateTable);
                const {name: tableName, schema, columns} = parsedTable.row;
                const tableIdentify = (schema || "public").toString() + "." + tableName.toString();
                
                // get primary key constraint from parsedTable.constraints
                // or from columns
                let primaryKeyConstraint = parsedTable.row.constraints.find(constraint => 
                    constraint instanceof PrimaryKeyConstraint
                ) as PrimaryKeyConstraint;
                if ( !primaryKeyConstraint ) {
                    parsedTable.row.columns.forEach(column => {
                        if ( column.get("primaryKey") ) {
                            primaryKeyConstraint = column.get("primaryKey");
                        }
                    });
                }
                
                let primaryKey = null;
                if ( primaryKeyConstraint ) {
                    primaryKey = primaryKeyConstraint.get("primaryKey").map(column => 
                        column.toString()
                    );
                }

                // table test (...) deprecated (...)
                const deprecatedColumns = parsedTable.row.deprecatedColumns.map(columnName =>
                    columnName.toString()
                );

                // table test (...) values (...)
                let rows = null;
                const parsedRows = parsedTable.get("values");
                if ( parsedRows && parsedRows.length ) {
                    rows = parsedRows;
                }

                // table (..., constraint x unique(...))
                const parsedUniqueConstraints = parsedTable.row.constraints.filter(constraint =>
                    constraint instanceof UniqueConstraint
                ) as UniqueConstraint[];

                parsedTable.get("columns").forEach(column => {
                    const uniqueConstraint = column.get("unique");
                    
                    if ( !uniqueConstraint ) {
                        return;
                    }

                    parsedUniqueConstraints.push(uniqueConstraint);
                });

                const uniqueConstraints = parsedUniqueConstraints.map(uniqueConstraint => {
                    let name = (
                        uniqueConstraint.get("name") &&
                        uniqueConstraint.get("name").toString()
                    );
                    if ( !name ) {
                        name = (
                            tableName.toString() + 
                            "_" +
                            uniqueConstraint.get("column").toString() + 
                            "_key"
                        );
                    }

                    return {
                        filePath,
                        identify: name,
                        name,
                        parsed: uniqueConstraint,
                        unique: uniqueConstraint.get("unique").map(column =>
                            column.toString()
                        )
                    };
                });

                const parsedCheckConstraints = parsedTable.row.constraints.filter(constraint =>
                    constraint instanceof CheckConstraint
                ) as CheckConstraint[];

                parsedTable.get("columns").forEach(column => {
                    const checkConstraint = column.get("check");

                    if ( !checkConstraint ) {
                        return;
                    }

                    parsedCheckConstraints.push(checkConstraint);
                });

                const checkConstraints = parsedCheckConstraints.map(checkConstraint => {
                    let name = (
                        checkConstraint.get("name") &&
                        checkConstraint.get("name").toString()
                    );

                    if ( !name ) {
                        name = (
                            tableName.toString() + 
                            "_" +
                            checkConstraint.get("column").toString() + 
                            "_check"
                        );
                    }

                    return {
                        filePath,
                        identify: name,
                        name,
                        parsed: checkConstraint
                    };
                });


                const tableModel = new TableModel({
                    filePath,
                    identify: tableIdentify,
                    name: tableName.toString(),
                    columns: columns.map(parseColumn => {
                        const key = parseColumn.get("name").toString();

                        return {
                            filePath,
                            identify: key,
                            key,
                            type: parseColumn.get("type").toString(),
                            parsed: parseColumn,
                            nulls: parseColumn.get("nulls")
                        };
                    }),
                    primaryKey,
                    deprecated: parsedTable.get("deprecated"),
                    deprecatedColumns,
                    rows,
                    uniqueConstraints,
                    checkConstraints,
                    parsed: parsedTable
                });

                objects.push(tableModel);
            }
        }

        return objects;
    }
}