"use strict";

const fs = require("fs");
const glob = require("glob");
const DdlCoach = require("./parser/DdlCoach");
const CreateTrigger = require("./parser/syntax/CreateTrigger");
const CreateFunction = require("./parser/syntax/CreateFunction");
const _ = require("lodash");

class DdlManager {
    static parseFolder(folderPath) {
        if ( !fs.existsSync(folderPath) ) {
            throw new Error(`folder "${ folderPath }" not found`);
        }

        // array of object:
        // {
        //   name: "some-file-name.sql",
        //   path: "/path/to/some-file-name.sql",
        //   content: {
        //        function: ...
        //   }
        // }
        let out = [];

        let files = glob.sync(folderPath + "/**/*.sql");
        files.forEach(filePath => {
            let fileName = filePath.split(/[/\\]/).pop();
            let content = DdlManager.parseFile(filePath);

            out.push({
                name: fileName,
                content
            });
        });

        return out;
    }

    static parseFile(filePath) {
        let fileContentBuffer;
        let fileContent;

        try {
            fileContentBuffer = fs.readFileSync(filePath);
        } catch(err) {
            throw new Error(`file "${ filePath }" not found`);
        }
        
        
        fileContent = fileContentBuffer.toString();
        fileContent = fileContent.trim();

        if ( fileContent === "" ) {
            return null;
        }

        let coach = new DdlCoach(fileContent);
        let func = coach.parseCreateFunction();

        let returns = func.returns.type;
        if ( func.returns.table ) {
            returns = {
                table: func.returns.table.map(arg => ({
                    name: arg.name,
                    type: arg.type
                }))
            };
        }

        let out = {
            function: {
                schema: func.schema,
                name: func.name,
                body: func.body.content,
                args: func.args.map(arg => ({
                    name: arg.name,
                    type: arg.type
                })),
                returns
            }
        };

        coach.skipSpace();
        if ( coach.is(";") ) {
            coach.expect(";");
            coach.skipSpace();

            if ( coach.isCreateTrigger() ) {
                let trigger = coach.parseCreateTrigger();
                
                // validate function name and trigger procedure
                if ( 
                    out.function.schema != trigger.procedure.schema ||
                    out.function.name != trigger.procedure.name
                ) {
                    throw new Error(`wrong procedure name ${
                        trigger.procedure.schema
                    }.${
                        trigger.procedure.name
                    }`);
                }

                // validate function returns type
                if ( out.function.returns !== "trigger" ) {
                    throw new Error(`wrong returns type ${ out.function.returns }`);
                }
            
                out.trigger = {
                    table: {
                        schema: trigger.table.schema,
                        name: trigger.table.name
                    }
                };
    
                if ( trigger.before ) {
                    out.trigger.before = true;
                }
                else if ( trigger.after ) {
                    out.trigger.after = true;
                }
    
                if ( trigger.insert ) {
                    out.trigger.insert = true;
                }
                if ( trigger.update ) {
                    if ( trigger.update === true ) {
                        out.trigger.update = true;
                    } else {
                        out.trigger.update = trigger.update.map(name => name);
                    }
                }
                if ( trigger.delete ) {
                    out.trigger.delete = true;
                }
            }
        }

        return out;
    }

    static async migrateFile(db, file) {
        if ( file == null ) {
            throw new Error("invalid function");
        }

        let ddlSql = CreateFunction.function2sql( file.function );
        ddlSql += ";";
        
        if ( file.trigger ) {
            let trigger = _.cloneDeep(file.trigger);
            trigger.procedure = {
                schema: file.function.schema,
                name: file.function.name
            };
            
            ddlSql += CreateTrigger.trigger2dropSql(trigger);
            ddlSql += ";";
            ddlSql += CreateTrigger.trigger2sql(trigger);
        }
        

        await db.query(ddlSql);
    }
}

module.exports = DdlManager;