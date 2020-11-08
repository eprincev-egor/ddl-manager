import { Syntax, Types } from "lang-coach";
import {
    CreateFunction,
    CreateTrigger,
    CommentOnFunction,
    CommentOnTrigger,
    GrapeQLCoach
} from "grapeql-lang";
import {
    function2identifySql,
    trigger2identifySql,
    function2identifyJson
} from "../utils";

// TODO: any => type
export class SqlFile extends (Syntax as any) {
    structure() {
        return {
            functions: Types.Array({
                element: CreateFunction
            }),
            triggers: Types.Array({
                element: CreateTrigger
            }),
            comments: Types.Array({
                element: Types.Or({
                    or: [
                        CommentOnFunction,
                        CommentOnTrigger
                    ]
                })
            })
        };
    }

    parse(coach: GrapeQLCoach, data: SqlFile["TInputData"]) {
        coach.skipSpace();

        data.functions = [];
        data.comments = [];
        data.triggers = [];
    
        this.parseFunctions( coach, data );

        this.parseTriggers( coach, data );
    }

    parseFunctions(coach: GrapeQLCoach, data: SqlFile["TInputData"]) {
        const func = coach.parse(CreateFunction);
        
        // TODO: any => type
        // check duplicate
        const isDuplicate = data.functions.some((prevFunc: any) =>
            function2identifySql( prevFunc )
            ===
            function2identifySql( func )
        );

        if ( isDuplicate ) {
            coach.throwError("duplicated function: " + function2identifySql( func ));
        }

        // two function inside file, can be with only same name and schema
        // TODO: any => type
        const isWrongName = data.functions.some((prevFunc: any) =>
            prevFunc.row.name !== func.row.name ||
            prevFunc.row.schema !== func.row.schema
        );

        if ( isWrongName ) {
            coach.throwError("two function inside file, can be with only same name and schema");
        }

        // save func
        data.functions.push(
            func
        );

        
        // comment on function
        if ( func.row.comment ) {
            data.comments.push(func.row.comment);
        }

        coach.skipSpace();
        coach.read(/[\s;]+/);

        if ( coach.is(CreateFunction) ) {
            this.parseFunctions( coach, data );
        }
    }

    parseTriggers(coach: GrapeQLCoach, data: SqlFile["TInputData"]) {
        coach.skipSpace();

        // skip spaces and some ;
        coach.read(/[\s;]+/);

        if ( !coach.is(CreateTrigger) ) {
            return;
        }

        const firstFunc = data.functions[0];
        if ( !firstFunc ) {
            coach.throwError("trigger inside file can be only with function");
        }

        // TODO: any => type
        const trigger = coach.parse(CreateTrigger) as any;

        // validate function name and trigger procedure
        if ( 
            firstFunc.row.schema !== trigger.row.procedure.row.schema ||
            firstFunc.row.name !== trigger.row.procedure.row.name
        ) {
            throw new Error(`wrong procedure name ${
                trigger.row.procedure.row.schema
            }.${
                trigger.row.procedure.row.name
            }`);
        }

        // TODO: any => type
        // validate function returns type
        const hasTriggerFunc = data.functions.some((func: any) =>
            func.row.returns.row.type === "trigger"
        );
        if ( !hasTriggerFunc ) {
            throw new Error("file must contain function with returns type trigger");
        }
        
        // TODO: any => type
        // check duplicate
        const isDuplicate = data.triggers.some((prevTrigger: any) =>
            trigger2identifySql( prevTrigger )
            ===
            trigger2identifySql( trigger )
        );

        if ( isDuplicate ) {
            coach.throwError("duplicated trigger: " + trigger2identifySql( trigger ));
        }

        data.triggers.push(trigger);

        // comment on trigger
        if ( trigger.row.comment ) {
            data.comments.push(trigger.row.comment);
        }

        this.parseTriggers( coach, data );
    }
    
    is(coach: GrapeQLCoach) {
        const i = coach.i;

        coach.skipSpace();
        const isSqlFile = coach.is(CreateFunction);

        coach.i = i;

        return isSqlFile;
    }
    
    toString() {
        let out = "";

        // TODO: any => type
        this.row.functions.forEach((func: any, i: number) => {
            if ( i > 0 ) {
                out += ";\n";
            }

            out += func.toString();

            if ( this.row.comments ) {
                const identifyJson = function2identifyJson( func );

                // TODO: any => type
                const funcComment = this.row.comments.find((comment: any) =>
                    comment.row.function &&
                    JSON.stringify(comment.row.function) === JSON.stringify( identifyJson )
                );
    
                if ( funcComment ) {
                    out += ";\n";
                    out += funcComment.toString();
                }
            }
        });

        if ( this.row.triggers ) {
            // TODO: any => type
            this.row.triggers.forEach((trigger: any) => {
                out += ";";
                out += trigger.toString();

                if ( this.row.comments ) {
                    // TODO: any => type
                    const triggerComment = this.row.comments.find((comment: any) =>
                        comment.row.trigger &&
                        comment.row.trigger.row.schema === trigger.row.table.row.schema &&
                        comment.row.trigger.row.table === trigger.row.table.row.name &&
                        comment.row.trigger.row.name === trigger.row.name
                    );

                    if ( triggerComment ) {
                        out += ";";
                        out += triggerComment.toString();
                    }
                }
            });
        }

        return out;
    }

}
