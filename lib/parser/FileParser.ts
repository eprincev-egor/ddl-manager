import {
    CreateFunction,
    CreateTrigger,
    GrapeQLCoach,
    CacheFor,
    CacheIndex as CacheIndexSyntax
} from "grapeql-lang";
import { IFileContent } from "../fs/File";
import { Cache } from "../ast";
import { CacheParser } from "./CacheParser";
import { Comment } from "../database/schema/Comment";
import { DatabaseFunction } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { replaceComments } from "./replaceComments";
import assert from "assert";

export class FileParser {

    static parse(sql: string) {
        const parser = new FileParser();
        return parser.parse(sql);
    }

    static parseFunction(sql: string) {
        const fileContent = FileParser.parse(sql) as IFileContent;
        assert.ok( fileContent, "should be not empty sql" );
        
        const func = (fileContent.functions || [])[0];
        assert.ok( func instanceof DatabaseFunction, "sql should contain function" );

        return func;
    }

    static parseCache(sql: string) {
        const fileContent = FileParser.parse(sql) as IFileContent;
        assert.ok( fileContent, "should be not empty sql" );
        
        const cache = (fileContent.cache || [])[0];
        assert.ok( cache instanceof Cache, "sql should contain cache" );

        return cache;
    }

    static parseIndexColumns(columnsStr: string) {
        const sql = `index some_type on (${columnsStr})`;
        const coach = new GrapeQLCoach(sql);
        const cacheIndex = coach.parse(CacheIndexSyntax);
        const columns = (cacheIndex.get("on") || []).map(elem => 
            elem.toString()
        );
        return columns;
    }

    parse(sql: string): IFileContent | undefined {
        const coach = replaceComments(sql);
        
        if ( coach.str.trim() === "" ) {
            return;
        }

        const state = this.parseFile(coach);
        return state;
    }


    private parseFile(coach: GrapeQLCoach) {
        coach.skipSpace();

        const state: IFileContent = {
            functions: [],
            triggers: [],
            cache: []
        };

        this.parseFunctions( coach, state );

        this.parseTriggers( coach, state );

        this.parseCache( coach, state );

        return state;
    }

    private parseFunctions(coach: GrapeQLCoach, state: IFileContent) {
        if ( !coach.is(CreateFunction) ) {
            return;
        }

        const funcJson = coach.parse(CreateFunction).toJSON() as any;
        const func = new DatabaseFunction({
            ...funcJson,
            body: funcJson.body.content,
            comment: Comment.fromFs({
                objectType: "function",
                dev: (
                    funcJson.comment ?
                        funcJson.comment.comment.content : 
                        undefined
                )
            })
        });
        
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

    private parseTriggers(coach: GrapeQLCoach, state: IFileContent) {
        coach.skipSpace();

        // skip spaces and some ;
        coach.read(/[\s;]+/);

        if ( !coach.is(CreateTrigger) ) {
            return;
        }

        const triggerJson = coach.parse(CreateTrigger).toJSON() as any;
        const trigger = new DatabaseTrigger({
            ...triggerJson,
            comment: Comment.fromFs({
                objectType: "trigger",
                dev: (
                    triggerJson.comment ?
                    triggerJson.comment.comment.content : 
                        undefined
                )
            })
        });

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

    private parseCache(coach: GrapeQLCoach, state: IFileContent) {
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

