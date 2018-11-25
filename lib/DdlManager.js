"use strict";

const DbState = require("./DbState");
const FilesState = require("./FilesState");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const CreateFunction = require("./parser/syntax/CreateFunction");
const _ = require("lodash");
const pg = require("pg");

const watchers = [];

class DdlManager {
    static async migrate(db, diff) {
        if ( diff == null ) {
            throw new Error("invalid diff");
        }

        let ddlSql = [];

        // drop old objects
        diff.drop.triggers.map(trigger => {
            ddlSql.push(
                CreateTrigger.trigger2dropSql(trigger)
            );
        });
        diff.drop.functions.map(func => {
            ddlSql.push(
                CreateFunction.function2dropSql(func)
            );
        });

        // create new objects
        diff.create.functions.forEach(func => {
            let funcIdentifySql = CreateFunction.function2identifySql( func );
           
            // check freeze object
            ddlSql.push(`
            do $$
                begin
                    if exists(
                        select from information_schema.routines as routines
        
                        left join pg_catalog.pg_proc as pg_proc on
                            routines.specific_name = pg_proc.proname || '_' || pg_proc.oid::text
        
                        where
                            routines.routine_schema = '${ func.schema }' and
                            routines.routine_name = '${ func.name }' and

                            pg_catalog.obj_description( pg_proc.oid ) 
                                is distinct from
                            'ddl-manager-sync'

                            and

                            -- function identify
                            -- public.func_name(type,type)
                            -- without args who has default value
                            (routines.routine_schema || '.' || routines.routine_name || '(' || coalesce((
                                select
                                    string_agg( type_name, ',' )
                                from (
                                    select
                                        format_type(type_id::oid,NULL) as type_name
                                    from unnest(
                                        string_to_array(proargtypes::text, ' ')
                                    ) as type_id
                                    
                                    limit (
                                        array_length( string_to_array(proargtypes::text, ' '), 1 ) -
                                        length(regexp_replace(proargdefaults, '[^}]', '', 'g'))
                                    )
                                ) as tmp
                            ),'') || ')')
                            =
                            '${funcIdentifySql}'
                    ) then
                        raise exception 'cannot replace freeze function ${ funcIdentifySql }';
                    end if;
                end
            $$;
            `);

            let sql = CreateFunction.function2sql( func );

            ddlSql.push( sql );
            ddlSql.push(`
                comment on function ${ funcIdentifySql } is 'ddl-manager-sync'
            `);
        });

        diff.create.triggers.forEach(trigger => {
            let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );

            // check freeze object
            ddlSql.push(`
            do $$
                begin
                    if exists(
                        select from pg_trigger

                        left join pg_class on 
                            pg_trigger.tgrelid = pg_class.oid

                        left join pg_namespace ON
                            pg_namespace.oid = pg_class.relnamespace
                        
                        where
                            pg_trigger.tgname = '${ trigger.name }' and
                            pg_namespace.nspname = '${ trigger.table.schema }' and
                            pg_class.relname = '${ trigger.table.name }' and
                            obj_description(pg_trigger.oid) 
                                is distinct from
                            'ddl-manager-sync'

                    ) then
                        raise exception 'cannot replace freeze trigger ${ triggerIdentifySql }';
                    end if;
                end
            $$;
            `);

            let sql;

            sql = CreateTrigger.trigger2dropSql( trigger );
            ddlSql.push( sql );
            
            sql = CreateTrigger.trigger2sql( trigger );
            ddlSql.push( sql );

            
            ddlSql.push(`
                comment on trigger ${ triggerIdentifySql } is 'ddl-manager-sync'
            `);
        });

        // and run
        ddlSql = ddlSql.join(";");
        
        await db.query(ddlSql);
    }

    static async build({db, folder}) {
        let needCloseConnect = false;

        // if db is config
        if ( !isDbClient(db) ) {
            db = await getDbClient(db);

            needCloseConnect = true;
        }
        
        let filesStateInstance = FilesState.create({
            folder
        });
        let filesState = {
            functions: filesStateInstance.getFunctions(),
            triggers: filesStateInstance.getTriggers()
        };
        
        let dbState = new DbState(db);
        await dbState.load();

        let diff = dbState.getDiff(filesState);


        // objects, who created without ddl-manager
        let hasFreezeObjects = (
            diff.freeze.functions.length ||
            diff.freeze.triggers.length
        );
        if ( hasFreezeObjects ) {
            let freezeObjects = [];

            diff.freeze.functions.forEach(freezeFunc => {
                let identifySql = CreateFunction.function2identifySql( freezeFunc );
                freezeObjects.push(
                    identifySql
                );
            });

            diff.freeze.triggers.forEach(freezeTrigger => {
                let identifySql = CreateTrigger.trigger2identifySql( freezeTrigger );
                freezeObjects.push(
                    identifySql
                );
            });


            console.error(`
                found objects without file mirror:
                    ${ freezeObjects.join("\n") }
            `);
        }

        await DdlManager.migrate(db, diff);

        if ( needCloseConnect ) {
            db.end();
        }

        logDiff(diff);
        
        console.log("ddl-manager build success");
        
        return filesStateInstance;
    }

    static async watch({db, folder}) {
        // if db is config
        if ( !isDbClient(db) ) {
            db = await getDbClient(db);
        }

        let filesState = await DdlManager.build({db, folder});

        await filesState.watch();
        
        filesState.on("change", async(diff) => {
            try {
                await DdlManager.migrate(db, diff);

                logDiff(diff);
            } catch(err) {
                console.log(err);
            }
        });

        filesState.on("error", err => {
            console.log(err.message);
        });

        watchers.push(filesState);
    }

    static stopWatch() {
        watchers.forEach(watcher => {
            watcher.stopWatch();
        });
        watchers.splice(0, watchers.length);
    }
}

function isDbClient(dbOrConfig) {
    return (
        dbOrConfig && 
        _.isFunction(dbOrConfig.query)
    );
}

async function getDbClient(dbConfig) {
    let config = {
        database: false,
        user: false,
        password: false,
        host: "localhost",
        port: 5432
    };

    if ( "database" in dbConfig ) {
        config.database = dbConfig.database;
    }
    if ( "user" in dbConfig ) {
        config.user = dbConfig.user;
    }
    if ( "password" in dbConfig ) {
        config.password = dbConfig.password;
    }
    if ( "host" in dbConfig ) {
        config.host = dbConfig.host;
    }
    if ( "port" in dbConfig ) {
        config.port = dbConfig.port;
    }

    let dbClient = new pg.Client(config);
    await dbClient.connect();

    return dbClient;
}

function logDiff(diff) {
    diff.drop.triggers.forEach(trigger => {
        let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );
        console.log("dropped trigger " + triggerIdentifySql);
    });

    diff.drop.functions.forEach(func => {
        let funcIdentifySql = CreateFunction.function2identifySql( func );
        console.log("dropped function " + funcIdentifySql);
    });
    
    diff.create.functions.forEach(func => {
        let funcIdentifySql = CreateFunction.function2identifySql( func );
        console.log("created function " + funcIdentifySql);
    });

    diff.create.triggers.forEach(trigger => {
        let triggerIdentifySql = CreateTrigger.trigger2identifySql( trigger );
        console.log("created trigger " + triggerIdentifySql);
    });
}

module.exports = DdlManager;