
import { IDBOSource, IDBODestination, IDBO } from "../common";
import { Migration } from "./Migration";
import { MigrationCommand } from "./MigrationCommand";
import { SimpleStrategy } from "./strategy/SimpleStrategy";

export interface IMigratorParams<TSource, TDestination> {
    source: TSource;
    destination: TDestination;
}

export class Migrator<TSource extends IDBOSource, TDestination extends IDBODestination> {
    private source: TSource;
    private destination: TDestination;
    
    private functionsStrategy: SimpleStrategy<IDBO>;
    private viewsStrategy: SimpleStrategy<IDBO>;
    private triggersStrategy: SimpleStrategy<IDBO>;

    constructor(params: IMigratorParams<TSource, TDestination>) {
        this.source = params.source;
        this.destination = params.destination;

        this.functionsStrategy = new SimpleStrategy();
        this.viewsStrategy = new SimpleStrategy();
        this.triggersStrategy = new SimpleStrategy();
    }

    async migrate() {
        const migration = this.buildMigration();
        
        for (const command of migration.commands) {
            await this.tryExecuteCommand(migration, command);
        }
    }

    private async tryExecuteCommand(migration: Migration, command: MigrationCommand) {
        try {
            await this.executeCommand(command);
        } catch(err) {
            migration.addError(err);
        }
    }

    private async executeCommand(command: MigrationCommand) {
        if ( command.type === "drop" ) {
            await this.destination.drop(command.object);
        }
        else {
            await this.destination.create(command.object);
        }
    }

    private buildMigration() {
        const migration = new Migration();

        const changes = this.source.state.compareWithDestination(this.destination.state);

        this.functionsStrategy.buildMigration(migration, changes.functions);
        this.viewsStrategy.buildMigration(migration, changes.views);
        this.triggersStrategy.buildMigration(migration, changes.triggers);

        return migration;
    }
}