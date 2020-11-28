"use strict";

const options_description = {
    "config":      "path to config.js file options,  default ./ddl-manager-config.js",
    "database":    "database name",
    "user":        "database user name",
    "password":    "database user password",
    "port":        "database port,  default: 5432",
    "host":        "database host,  default: localhost",
    "folder":      "path to directory with *.sql files for dump or build,  default ./ddl",
    "unfreeze":    `true or false,   
                            it dump option marking all database objects (funcs, triggers)  
                            give permissions sync with build or watch`,
    "help":         "help about command"
};

const commands_help = {
    build: {
        description: "migrate functions and triggers from files into database",
        options: ["config", "database", "user", "password", "port", "host", "folder", "help"]
    },
    watch: {
        description: "build and watching folder for changes",
        options: ["config", "database", "user", "password", "port", "host", "folder", "help"]
    },
    dump:  {     
        description: " write functions and triggers from database into folder",
        options: ["config", "database", "user", "password", "port", "host", "folder", "unfreeze", "help"]
    },
    "refresh-cache":  {     
        description: " update all cache columns to actual values",
        options: ["config", "database", "user", "password", "port", "host", "folder", "help"]
    }
};

module.exports = function(args, commandName) {
    console.log(`
    usage:  
        $ ddl-manager ${ commandName || "[command]" } [options]  
    `);

    console.log(" commands:");

    let commandsOrder = ["build", "watch", "dump", "refresh-cache"];
    if ( commandName ) {
        commandsOrder = [commandName];
    }
    
    commandsOrder.forEach(commandName => {
        let commandHelp = commands_help[ commandName ];
        console.log(`    ${ commandName }    ${ commandHelp.description }`);
        console.log("");
        console.log(`    ${ commandName } options:`);
        commandHelp.options.forEach(optionName => {
            let optionDescription = options_description[ optionName ];

            let spaces = "    ";
            for (let i = 0; i < "database".length - optionName.length; i++) {
                spaces += " ";
            }
            console.log(`        --${optionName}${ spaces }${ optionDescription }`);
        });

        console.log("");
        console.log("");
    });

    if ( !commandName ) {
        console.log(`
    example:  
        $ ddl-manager build          --config=ddl-manager-config.js  
        $ ddl-manager watch          --config=ddl-manager-config.js  
        $ ddl-manager dump           --config=ddl-manager-config.js  --unfreeze=true  
        $ ddl-manager refresh-cache  --config=ddl-manager-config.js
        `);
    }
};