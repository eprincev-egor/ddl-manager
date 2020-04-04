import pg from "pg";
import {DBDriver} from "./DBDriver";
import { FunctionModel } from "../objects/FunctionModel";
import { PgParser } from "../parser/pg/PgParser";
import { TriggerModel } from "../objects/TriggerModel";

export class PgDBDriver 
extends DBDriver {
    private db: pg.Client;
    private parser: PgParser;
    
    constructor(options: DBDriver["options"]) {
        super(options);
        this.parser = new PgParser();
    }

    async connect() {
        this.db = new pg.Client(this.options);
        await this.db.connect();
    }

    async loadFunctions(): Promise<FunctionModel[]> {
        const sql = `
        select
            pg_get_functiondef( pg_proc.oid ) as ddl,
            pg_catalog.obj_description( pg_proc.oid ) as comment
        from information_schema.routines as routines
        left join pg_catalog.pg_proc as pg_proc on
            routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text
        
        -- ignore language C or JS
        inner join pg_catalog.pg_language as pg_language on
            pg_language.oid = pg_proc.prolang and
            lower(pg_language.lanname) in ('sql', 'plpgsql')
        
        where
            routines.routine_schema <> 'pg_catalog' and
            routines.routine_schema <> 'information_schema' and
            routines.routine_definition is distinct from 'aggregate_dummy' and
            not exists(
                select from pg_catalog.pg_aggregate as pg_aggregate
                where
                    pg_aggregate.aggtransfn = pg_proc.oid or
                    pg_aggregate.aggfinalfn = pg_proc.oid
            )
        
        order by
            routines.routine_schema, 
            routines.routine_name
        `;
        
        const result = await this.db.query<{
            ddl: string;
            comment: string;
        }>(sql);

        const outputFunctions: FunctionModel[] = [];
        result.rows.forEach(row => {
            const functionDDL = row.ddl;
            const models = this.parser.parseFile("(database)", functionDDL);
            const functionModel = models[0] as FunctionModel;

            functionModel.set({
                createdByDDLManager: false
            });

            outputFunctions.push(functionModel);
        });

        return outputFunctions;
    }

    async loadTriggers(): Promise<TriggerModel[]> {
        const sql = `
            select
                pg_get_triggerdef( pg_trigger.oid ) as ddl,
                pg_catalog.obj_description( pg_trigger.oid ) as comment
            from pg_trigger
            where
                pg_trigger.tgisinternal = false
        `;
        
        const result = await this.db.query<{
            ddl: string;
            comment: string;
        }>(sql);

        const outputTriggers: TriggerModel[] = [];
        result.rows.forEach(row => {
            const triggerDDL = row.ddl;
            const models = this.parser.parseFile("(database)", triggerDDL);
            const triggerModel = models[0] as TriggerModel;

            triggerModel.set({
                createdByDDLManager: false
            });

            outputTriggers.push(triggerModel);
        });

        return outputTriggers;
    }
}