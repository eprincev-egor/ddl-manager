import {IDBOParser, IDBO} from "../../../common";
import {
    GrapeQLCoach,
    CreateFunction,
    CreateTrigger,
    CreateView,
    CreateTable,
    Extension as GQLExtension
} from "grapeql-lang";
import { parseFunction } from "./parseFunction";
import { parseTrigger } from "./parseTrigger";
import { parseView } from "./parseView";
import { parseTable } from "./parseTable";
import { parseExtension } from "./parseExtension";
import { Extension as PGExtension } from "../objects/Extension";

export class PostgresParser 
implements IDBOParser {
    parse(sql: string) {
        const coach = new GrapeQLCoach(sql);
        const objects: (IDBO | PGExtension)[] = [];

        for (; coach.i < coach.str.length; coach.i++) {
            // need ignore comments (inside comment can be invalid syntax)
            coach.skipSpace();
            
            const dbo = this.parseDBO(coach);
            if ( dbo ) {
                objects.push(dbo);
            }
        }

        return objects;
    }

    private parseDBO(coach: GrapeQLCoach) {
        // create or replace function
        if ( coach.is(CreateFunction) ) {
            return parseFunction(coach);
        }

        // create trigger
        if ( coach.is(CreateTrigger) ) {
            return parseTrigger(coach);
        }

        // create view
        if ( coach.is(CreateView) ) {
            return parseView(coach);
        }

        // create table
        if ( coach.is(CreateTable) ) {
            return parseTable(coach);
        }

        // extension
        if ( coach.is(GQLExtension) ) {
            return parseExtension(coach);
        }
    }
}