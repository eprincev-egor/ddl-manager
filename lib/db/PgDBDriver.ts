import pg from "pg";
import {DBDriver} from "./DBDriver";
import { FunctionModel } from "../objects/FunctionModel";
import { PgParser } from "../parser/pg/PgParser";
import { TriggerModel } from "../objects/TriggerModel";
import { TableModel } from "../objects/TableModel";
import { ColumnModel } from "../objects/ColumnModel";
import { UniqueConstraintModel } from "../objects/UniqueConstraintModel";
import { CheckConstraintModel } from "../objects/CheckConstraintModel";
import { GrapeQLCoach, Expression } from "grapeql-lang";
import { ForeignKeyConstraintModel } from "../objects/ForeignKeyConstraintModel";

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

    async end() {
        await this.db.end();
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

    async loadTables(): Promise<TableModel[]> {
        const selectColumnsSQL = `
            select
                pg_columns.table_schema,
                pg_columns.table_name,
                pg_columns.column_name,
                pg_columns.column_default,
                pg_columns.data_type,
                pg_columns.is_nullable
            from information_schema.columns as pg_columns
            where
                pg_columns.table_schema != 'pg_catalog' and
                pg_columns.table_schema != 'information_schema'
            
            order by pg_columns.ordinal_position
        `;
        const columnsResult = await this.db.query<{
            table_schema: string;
            table_name: string;
            column_name: string;
            column_default: string;
            data_type: string;
            is_nullable: "YES" | "NO" | null;
        }>(selectColumnsSQL);

        const outputTables: TableModel[] = [];
        const schemas = {};
        for (const columnRow of columnsResult.rows) {
            const {
                table_schema: schemaName,
                table_name: tableName,
                column_name: columnKey,
                column_default: columnDefault,
                data_type: columnType,
                is_nullable
            } = columnRow;
            
            let schema = schemas[ schemaName ];
            if ( !schema ) {
                schema = {
                    name: schemaName,
                    tables: {}
                };
                schemas[ schemaName ] = schema;
            }

            let tableModel: TableModel = schema.tables[ tableName ];
            if ( !tableModel ) {
                tableModel = new TableModel({
                    identify: `${schemaName}.${tableName}`,
                    filePath: "(database)",
                    name: tableName,
                    columns: []
                });
                schema.tables[ tableName ] = tableModel;

                outputTables.push(tableModel);
            }

            
            const columnModel = new ColumnModel({
                identify: columnKey,
                key: columnKey,
                type: columnType,
                default: columnDefault,
                nulls: (
                    is_nullable === "YES" ? 
                        true : 
                        false
                )
            });
            tableModel.setColumn(columnKey, columnModel);
        }

        const constraintsSQL = `
            select
                tc.constraint_type,
                tc.constraint_name,
                tc.table_schema,
                tc.table_name,
                rc.update_rule,
                rc.delete_rule,
                (
                    select
                        array_agg(kc.column_name::text)
                    from information_schema.key_column_usage as kc
                    where
                        kc.table_name = tc.table_name and
                        kc.table_schema = tc.table_schema and
                        kc.constraint_name = tc.constraint_name
                ) as columns,
                fk_info.columns as reference_columns,
                fk_info.table_name as reference_table,
                check_info.check_clause

            from information_schema.table_constraints as tc

            left join information_schema.referential_constraints as rc on
                rc.constraint_schema = tc.constraint_schema and
                rc.constraint_name = tc.constraint_name
            
            left join lateral (
                select
                    ( 
                        array_agg( distinct 
                            ccu.table_schema::text || '.' ||
                            ccu.table_name::text 
                        ) 
                    )[1] as table_name,
                    array_agg( ccu.column_name::text ) as columns
                from information_schema.constraint_column_usage as ccu
                where
                    ccu.constraint_name = rc.constraint_name and
                    ccu.constraint_schema = rc.constraint_schema
            ) as fk_info on true

            left join information_schema.check_constraints as check_info on
                check_info.constraint_schema = tc.table_schema and
                check_info.constraint_name = tc.constraint_name
            

            where
                tc.table_schema != 'pg_catalog' and
                tc.table_schema != 'information_schema' and
                (
                    tc.constraint_type != 'CHECK'
                    or
                    tc.constraint_name not ilike '%_not_null' and
                    check_info.check_clause not ilike '% IS NOT NULL'
                )
        `;
        const constraintsResult = await this.db.query<{
            constraint_type: "FOREIGN KEY" | "UNIQUE" | "PRIMARY KEY" | "CHECK";
            constraint_name: string;
            table_schema: string;
            table_name: string;
            update_rule: string;
            delete_rule: string;
            columns: string[];
            reference_columns: string[];
            reference_table: string;
            check_clause: string;
        }>(constraintsSQL);

        for (const constraintRow of constraintsResult.rows) {
            const {
                constraint_type: constraintType,
                constraint_name: constraintName,
                table_schema: schemaName,
                table_name: tableName,
                columns: constraintColumns
            } = constraintRow;

            const schema = schemas[ schemaName ];
            const tableModel: TableModel = schema.tables[ tableName ];

            if ( constraintType === "PRIMARY KEY" ) {
                tableModel.set({
                    primaryKey: constraintColumns
                });
            }

            if ( constraintType === "UNIQUE" ) {
                const uniqueConstraintModel = new UniqueConstraintModel({
                    identify: constraintName,
                    name: constraintName,
                    unique: constraintColumns
                });
                tableModel.addUniqueConstraint(uniqueConstraintModel);
            }

            if ( constraintType === "CHECK" ) {
                const checkString = extrudeBracketsFromCheckClause(constraintRow.check_clause);
                const checkConstraintModel = new CheckConstraintModel({
                    identify: constraintName,
                    name: constraintName,
                    check: checkString.trim()
                });
                tableModel.addCheckConstraint(checkConstraintModel);
            }

            if ( constraintType === "FOREIGN KEY" ) {
                const foreignKeyModel = new ForeignKeyConstraintModel({
                    identify: constraintName,
                    name: constraintName,
                    columns: constraintColumns,
                    referenceColumns: constraintRow.reference_columns,
                    referenceTableIdentify: constraintRow.reference_table
                });
                tableModel.addForeignKeyConstraint(foreignKeyModel);
            }
        }

        return outputTables;
    }

    async dropFunction(functionModel: FunctionModel) {
        const functionIdentify = functionModel.getIdentify();
        await this.db.query(`
            drop function if exists ${functionIdentify};
        `);
    }

    async createFunction(functionModel: FunctionModel) {
        const parsedFunction = functionModel.get("parsed");
        await this.db.query( parsedFunction.toString() );
    }

    async dropTrigger(triggerModel: TriggerModel) {
        const triggerIdentify = triggerModel.getIdentify();
        await this.db.query(`
            drop trigger if exists ${triggerIdentify};
        `);
    }

    async createTrigger(triggerModel: TriggerModel) {
        const parsedTrigger = triggerModel.get("parsed");
        await this.db.query( parsedTrigger.toString() );
    }
}

function extrudeBracketsFromCheckClause(checkClause: string): string {
    const coach = new GrapeQLCoach(checkClause);
    const expression = coach.parse(Expression);
    const checkString = expression.toString();
    return checkString;
}