import {
    CreateFunction,
    CreateTrigger,
    GrapeQLCoach,
    Comment
} from "grapeql-lang";
import { IState } from "../interface";
import {
    function2identifySql,
    trigger2identifySql
} from "../utils";
import { DatabaseFunction } from "../ast/DatabaseFunction";
import { DatabaseTrigger } from "../ast/DatabaseTrigger";

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
            triggers: []
        };

        this.parseFunctions( coach, state );

        this.parseTriggers( coach, state );

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
        
        // TODO: any => type
        // check duplicate
        const isDuplicate = state.functions.some((prevFunc: any) =>
            function2identifySql( prevFunc )
            ===
            function2identifySql( func )
        );

        if ( isDuplicate ) {
            coach.throwError("duplicated function: " + function2identifySql( func ));
        }

        // two function inside file, can be with only same name and schema
        // TODO: any => type
        const isWrongName = state.functions.some((prevFunc: any) =>
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

        // TODO: any => type
        // check duplicate
        const isDuplicate = state.triggers.some((prevTrigger: any) =>
            trigger2identifySql( prevTrigger )
            ===
            trigger2identifySql( trigger )
        );

        if ( isDuplicate ) {
            coach.throwError("duplicated trigger: " + trigger2identifySql( trigger ));
        }

        state.triggers.push( trigger );

        this.parseTriggers( coach, state );
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
