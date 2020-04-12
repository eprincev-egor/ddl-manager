import { CreateTable } from "grapeql-lang";
import { parsePrimaryKey } from "./parsePrimaryKey";
import { parseUniqueConstraints } from "./parseUniqueConstraints";
import { parseCheckConstraints } from "./parseCheckConstraints";
import { parseForeignKeys } from "./parseForeignKeys";

export function parseConstraints(
    tableIdentify: string, 
    tableConstraints: CreateTable["row"]["constraints"]
) {

    // PRIMARY KEY
    const primaryKey = parsePrimaryKey(tableIdentify, tableConstraints);
    
    // UNIQUE
    const uniqueConstraints = parseUniqueConstraints(
        tableIdentify, 
        tableConstraints
    );

    // CHECK
    const checkConstraints = parseCheckConstraints(
        tableIdentify, 
        tableConstraints
    );
    
    // FOREIGN KEY
    const foreignKeys = parseForeignKeys(
        tableIdentify, 
        tableConstraints
    );


    const constraints = [
        primaryKey,
        ...uniqueConstraints,
        ...checkConstraints,
        ...foreignKeys
    ].filter(constraint => constraint != null);

    return constraints;
}