import {
    CreateFunction,
    CreateTrigger,
    GrapeQLCoach,
    Comment,
    CacheFor
} from "grapeql-lang";
import { IState } from "../interface";
import { DatabaseFunction, DatabaseTrigger } from "../ast";
import assert from "assert";
import { CacheParser } from "./CacheParser";

export class FileParser {

    static parse(sql: string) {
        const parser = new FileParser();
        return parser.parse(sql);
    }

    parse(sql: string): IState | undefined {
        const coach = replaceComments(sql);
        
        if ( coach.str.trim() === "" ) {
            return;
        }

        const state = this.parseFile(coach);
        return state;
    }


    private parseFile(coach: GrapeQLCoach) {
        coach.skipSpace();

        const state: IState = {
            functions: [],
            triggers: [],
            cache: []
        };

        this.parseFunctions( coach, state );

        this.parseTriggers( coach, state );

        this.parseCache( coach, state );

        return state;
    }

    private parseFunctions(coach: GrapeQLCoach, state: IState) {
        if ( !coach.is(CreateFunction) ) {
            return;
        }

        const funcJson = coach.parse(CreateFunction).toJSON() as any;
        const func = new DatabaseFunction(funcJson);
        if ( funcJson.comment ) {
            func.comment = funcJson.comment.comment.content;
        }
        func.body = funcJson.body.content;
        
        // check duplicate
        const isDuplicate = state.functions.some((prevFunc) =>
            prevFunc.getSignature()
            ===
            func.getSignature()
        );

        if ( isDuplicate ) {
            coach.throwError("duplicated function: " + func.getSignature() );
        }

        // two function inside file, can be with only same name and schema
        const isWrongName = state.functions.some((prevFunc) =>
            prevFunc.name !== func.name ||
            prevFunc.schema !== func.schema
        );

        if ( isWrongName ) {
            coach.throwError("two function inside file, can be with only same name and schema");
        }

        // save func
        state.functions.push(
            func
        );

        coach.skipSpace();
        coach.read(/[\s;]+/);

        if ( coach.is(CreateFunction) ) {
            this.parseFunctions( coach, state );
        }
    }

    private parseTriggers(coach: GrapeQLCoach, state: IState) {
        coach.skipSpace();

        // skip spaces and some ;
        coach.read(/[\s;]+/);

        if ( !coach.is(CreateTrigger) ) {
            return;
        }

        // TODO: any => type
        const triggerJson = coach.parse(CreateTrigger).toJSON() as any;
        const trigger = new DatabaseTrigger(triggerJson);
        if ( triggerJson.comment ) {
            trigger.comment = triggerJson.comment.comment.content;
        }

        // validate function name and trigger procedure
        const firstFunc = state.functions[0];
        if ( firstFunc ) {
            if (
                firstFunc.schema !== trigger.procedure.schema ||
                firstFunc.name !== trigger.procedure.name
            ) {
                throw new Error(`wrong procedure name ${
                    trigger.procedure.schema
                }.${
                    trigger.procedure.name
                }`);
            }

            // validate function returns type
            const hasTriggerFunc = state.functions.some((func: any) =>
                func.returns.type === "trigger"
            );
            if ( !hasTriggerFunc ) {
                throw new Error("file must contain function with returns type trigger");
            }
        }

        // check duplicate
        const isDuplicate = state.triggers.some((prevTrigger) =>
            prevTrigger.getSignature()
            ===
            trigger.getSignature()
        );

        if ( isDuplicate ) {
            coach.throwError("duplicated trigger: " + trigger.getSignature() );
        }

        state.triggers.push( trigger );

        this.parseTriggers( coach, state );
    }

    private parseCache(coach: GrapeQLCoach, state: IState) {
        assert.ok(state.cache);
        coach.skipSpace();

        // skip spaces and some ;
        coach.read(/[\s;]+/);

        if ( !coach.is(CacheFor) ) {
            return;
        }

        const cache = CacheParser.parse(coach);
        state.cache.push(cache);

        this.parseCache(coach, state);
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
