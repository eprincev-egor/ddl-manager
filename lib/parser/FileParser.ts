import {
    CreateFunction,
    CreateFunctionArgument,
    CreateTrigger,
    Cursor,
    Sql
} from "psql-lang";
import { IFileContent } from "../fs/File";
import { Cache } from "../ast";
import { CacheParser } from "./CacheParser";
import { Comment } from "../database/schema/Comment";
import { DatabaseFunction, IDatabaseFunctionArgument, IDatabaseFunctionReturns } from "../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../database/schema/DatabaseTrigger";
import { TableID } from "../database/schema/TableID";
import { CacheSyntax } from "./CacheSyntax";
import { DEFAULT_SCHEMA } from "./defaults";
import { FileSyntax } from "./FileSyntax";
import assert from "assert";

export class FileParser {

    static parse(sql: string) {
        const parser = new FileParser();
        return parser.parseSql(sql);
    }

    static parseFile(filePath: string) {
        const parser = new FileParser();
        return parser.parseFile(filePath);
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

    parseFile(filePath: string) {
        const {cursor} = Sql.file(filePath);
        return this.parse(cursor);
    }

    parseSql(sql: string) {        
        const {cursor} = Sql.code(sql);
        return this.parse(cursor);
    }

    private parse(cursor: Cursor) {
        const content = cursor.parse(FileSyntax);

        const state: IFileContent = {
            functions: [],
            triggers: [],
            cache: content.row.caches.map(buildCache)
        };
        for (const funcNode of content.row.functions) {
            addFunction(cursor, state, funcNode);
        }

        for (const triggerNode of content.row.triggers) {
            addTrigger(cursor, state, triggerNode);
        }

        return state;
    }
}

function addFunction(
    cursor: Cursor,
    state: IFileContent,
    funcNode: CreateFunction
) {
    const func = buildFunction(funcNode);

    // check duplicate
    const isDuplicate = state.functions.some((prevFunc) =>
        prevFunc.getSignature()
        ===
        func.getSignature()
    );

    if ( isDuplicate ) {
        cursor.throwError("duplicated function: " + func.getSignature(), funcNode );
    }

    // two function inside file, can be with only same name and schema
    const isWrongName = state.functions.some((prevFunc) =>
        prevFunc.name !== func.name ||
        prevFunc.schema !== func.schema
    );

    if ( isWrongName ) {
        cursor.throwError("two function inside file, can be with only same name and schema", funcNode);
    }

    state.functions.push(func);
}

function addTrigger(
    cursor: Cursor,
    state: IFileContent,
    triggerNode: CreateTrigger
) {
    const trigger = buildTrigger(triggerNode);

    // validate function name and trigger procedure
    const firstFunc = state.functions[0];
    if ( firstFunc ) {
        if (
            firstFunc.schema !== trigger.procedure.schema ||
            firstFunc.name !== trigger.procedure.name
        ) {
            const wrongName = `${trigger.procedure.schema}.${trigger.procedure.name}`;
            cursor.throwError(`wrong procedure name ${wrongName}`, triggerNode);
        }

        // validate function returns type
        const hasTriggerFunc = state.functions.some((func: any) =>
            func.returns.type === "trigger"
        );
        if ( !hasTriggerFunc ) {
            cursor.throwError("file must contain function with returns type trigger", triggerNode);
        }
    }

    // check duplicate
    const isDuplicate = state.triggers.some((prevTrigger) =>
        prevTrigger.getSignature()
        ===
        trigger.getSignature()
    );

    if ( isDuplicate ) {
        cursor.throwError("duplicated trigger: " + trigger.getSignature(), triggerNode );
    }

    state.triggers.push( trigger );
}

function buildFunction(funcNode: CreateFunction) {
    return new DatabaseFunction({
        schema: funcNode.row.schema?.toString() || DEFAULT_SCHEMA,
        name: funcNode.row.name.toString(),
        args: funcNode.row.args.map(parseArg),
        returns: parseReturns(funcNode),
        body: funcNode.row.body.row.string,
        language: funcNode.row.language,
        immutable: funcNode.row.immutability === "immutable",
        returnsNullOnNull: funcNode.row.inputNullsRule === "returns null on null input",
        stable: funcNode.row.immutability === "stable",
        strict: funcNode.row.inputNullsRule === "strict",
        parallel: funcNode.row.parallel ? [funcNode.row.parallel] : undefined,
        cost: Number(funcNode.row.cost?.toString()) || undefined,
        comment: Comment.fromFs({
            objectType: "function"
        })
    });
}

function buildTrigger(triggerNode: CreateTrigger) {
    return new DatabaseTrigger({
        name: triggerNode.row.name.toString(),
        table: TableID.fromString(
            triggerNode.row.table.toString()
        ),
        procedure: {
            schema: triggerNode.row.procedure.row.schema?.toString() || DEFAULT_SCHEMA,
            name: triggerNode.row.procedure.row.name?.toString(),
            args: []
        },
        before: triggerNode.row.events.before,
        after: !triggerNode.row.events.before,
        insert: triggerNode.row.events.insert,
        update: !!triggerNode.row.events.update,
        updateOf: Array.isArray(triggerNode.row.events.update) ?
            triggerNode.row.events.update
                .map(name => name.toValue())
                .sort() : 
                undefined,
        delete: triggerNode.row.events.delete,

        when: triggerNode.row.when?.toString(),
        constraint: triggerNode.row.constraint,
        deferrable: triggerNode.row.deferrable,
        statement: triggerNode.row.statement,
        initially: triggerNode.row.initially,

        comment: Comment.fromFs({
            objectType: "trigger"
        })
    });
}

function buildCache(cacheSyntax: CacheSyntax) {
    return CacheParser.parse(cacheSyntax);
}

function parseReturns(funcNode: CreateFunction): IDatabaseFunctionReturns {
    const returns = funcNode.row.returns.row;

    if ( "table" in returns ) {
        return {
            setof: returns.setOf,
            table: returns.table.map(parseArg)
        }
    }

    return {
        setof: returns.setOf,
        type: returns.type.toString()
    };
}

function parseArg(argNode: CreateFunctionArgument): IDatabaseFunctionArgument {
    return {
        out: argNode.row.out,
        in: argNode.row.in,
        name: argNode.row.name?.toString(),
        type: argNode.row.type.toString(),
        default: argNode.row.default?.toString()
    };
}