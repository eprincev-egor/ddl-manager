import {
    CreateFunction,
    CreateTrigger,
    GrapeQLCoach,
    Comment
} from "grapeql-lang";
import { SqlFile } from "./SqlFile";

export class FileParser {

    static parse(sql: string) {
        const parser = new FileParser();
        return parser.parse(sql);
    }

    parse(sql: string) {
        const coach = replaceComments(sql);
        
        if ( coach.str.trim() === "" ) {
            return;
        }

        const sqlFile = coach.parse(SqlFile as any) as SqlFile;
        return sqlFile;
    }
}


function replaceComments(sql: string) {
    const coach = new GrapeQLCoach(sql);

    const startIndex = coach.i;
    const newStr = coach.str.split("");

    for (; coach.i < coach.n; coach.i++) {
        const i = coach.i;

        // ignore comments inside function
        if ( coach.is(CreateFunction) ) {
            coach.parse(CreateFunction);
            coach.i--;
            continue;
        }

        if ( coach.is(CreateTrigger) ) {
            coach.parse(CreateTrigger);
            coach.i--;
            continue;
        }

        if ( coach.is(Comment) ) {
            coach.parse(Comment);

            const length = coach.i - i;
            // safe \n\r
            const spaceStr = coach.str.slice(i, i + length).replace(/[^\n\r]/g, " ");

            newStr.splice(i, length, ...spaceStr.split("") );
            
            coach.i--;
            continue;
        }
    }

    coach.i = startIndex;
    coach.str = newStr.join("");

    return coach;
}
