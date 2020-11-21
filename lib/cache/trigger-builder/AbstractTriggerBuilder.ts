import {
    Table,
    Cache,
    DatabaseTrigger,
    DatabaseFunction,
    AbstractAstElement,
    Expression
} from "../../ast";
import { buildReferenceMeta, IReferenceMeta } from "./condition/buildReferenceMeta";
import { Database as DatabaseStructure } from "../schema/Database";
import { buildFromAndWhere } from "../processor/buildFromAndWhere";
import { ConditionBuilder } from "./condition/ConditionBuilder";

export abstract class AbstractTriggerBuilder {
    protected readonly cache: Cache;
    protected readonly databaseStructure: DatabaseStructure;
    protected readonly triggerTable: Table;
    protected readonly triggerTableColumns: string[];
    protected readonly referenceMeta: IReferenceMeta;
    protected readonly from: string[];
    protected readonly where: Expression;
    protected readonly conditionBuilder: ConditionBuilder;

    constructor(
        cache: Cache,
        databaseStructure: DatabaseStructure,
        triggerTable: Table,
        triggerTableColumns: string[]
    ) {
        this.cache = cache;
        this.databaseStructure = databaseStructure;
        this.triggerTable = triggerTable;
        this.triggerTableColumns = triggerTableColumns;
        
        this.conditionBuilder = new ConditionBuilder(
            cache,
            triggerTable,
            triggerTableColumns,
            databaseStructure
        );
        
        this.referenceMeta = buildReferenceMeta(
            this.cache,
            this.triggerTable
        );
        const {from, where} = buildFromAndWhere(
            this.cache,
            this.triggerTable
        );
        this.from = from;
        this.where = where;
    }

    createTrigger(): {
        trigger: DatabaseTrigger;
        function: DatabaseFunction;
    } {
        return {
            trigger: this.createDatabaseTrigger(),
            function: this.createDatabaseFunction()
        };
    }

    private createDatabaseFunction() {

        const func = new DatabaseFunction({
            schema: "public",
            name: this.generateTriggerName(),
            body: "\n" + this.createBody().toSQL() + "\n",
            comment: "cache",
            args: [],
            returns: {type: "trigger"}
        });
        return func;
    }

    private createDatabaseTrigger() {
        
        const updateOfColumns = this.triggerTableColumns
            .filter(column =>  column !== "id" )
            .sort();
        
        const trigger = new DatabaseTrigger({
            name: this.generateTriggerName(),
            after: true,
            insert: true,
            delete: true,
            update: updateOfColumns.length > 0,
            updateOf: updateOfColumns,
            procedure: {
                schema: "public",
                name: this.generateTriggerName(),
                args: []
            },
            table: {
                schema: this.triggerTable.schema || "public",
                name: this.triggerTable.name
            }
        });

        return trigger;
    }

    private generateTriggerName() {
        const triggerName = [
            "cache",
            this.cache.name,
            "for",
            this.cache.for.table.name,
            "on",
            this.triggerTable.name
        ].join("_");
        return triggerName;
    }

    protected abstract createBody(): AbstractAstElement;
}