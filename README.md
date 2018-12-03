# ddl-manager  
store your postgres procedures and triggers in files  

```
usage:  
    $ ddl-manager [command] [options]  

command:  
    build    migrate functions and triggers from files into database    
    watch    build and watching folder for changes  
    dump     write functions and triggers from database into folder  
  
options:  
    --config      path to config.js file options,  default ./ddl-manager-config.js   
    --database    database name  
    --user        database user name  
    --password    database user password  
    --port        database port,  default: 5432  
    --host        database host,  default: localhost  
    --folder      path to directory with *.sql files for dump or build,  default ./ddl  
  
dump options:  
    --unfreeze    true or false,   
                        it dump option marking all database objects (funcs, triggers)  
                        give permissions sync with build or watch  
  
example:  
    $ ddl-manager build --config=ddl-manager-config.js  
    $ ddl-manager watch --config=ddl-manager-config.js  
    $ ddl-manager dump  --config=ddl-manager-config.js  --unfreeze=true  
```