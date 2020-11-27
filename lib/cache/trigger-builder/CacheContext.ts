import {
    Table,
    Expression,
    Cache
} from "../../ast";
import { Database as DatabaseStructure } from "../../database/schema/Database";

export interface IReferenceMeta {
    columns: string[];
    expressions: Expression[];
    filters: Expression[];
}

export class CacheContext {
    readonly cache: Cache;
    readonly triggerTable: Table;
    readonly triggerTableColumns: string[];
    readonly databaseStructure: DatabaseStructure;
    readonly referenceMeta: IReferenceMeta;
    
    constructor(
        cache: Cache,
        triggerTable: Table,
        triggerTableColumns: string[],
        databaseStructure: DatabaseStructure
    ) {
        this.cache = cache;
        this.triggerTable = triggerTable;
        this.triggerTableColumns = triggerTableColumns;
        this.databaseStructure = databaseStructure;
        this.referenceMeta = this.buildReferenceMeta();
    }

    getTableReferencesToTriggerTable() {
        const tableReferences = this.cache.select.getAllTableReferences()
            .filter(tableRef =>
                tableRef.table.equal(this.triggerTable)
            );

        return tableReferences;
    }

    private buildReferenceMeta(): IReferenceMeta {

        const referenceMeta: IReferenceMeta = {
            columns: [],
            filters: [],
            expressions: []
        };

        const where = this.cache.select.where;
        if ( !where ) {
            return referenceMeta;
        }

        for (const andCondition of where.splitBy("and")) {
            const conditionColumns = andCondition.getColumnReferences();

            const columnsFromCacheTable = conditionColumns.filter(columnRef =>
                columnRef.tableReference.equal(this.cache.for)
            );
            const columnsFromTriggerTable = conditionColumns.filter(columnRef =>
                columnRef.tableReference.table.equal(this.triggerTable)
            );

            const isReference = (
                columnsFromCacheTable.length
                &&
                columnsFromTriggerTable.length
            );

            if ( isReference ) {
                referenceMeta.expressions.push(
                    andCondition
                );
                referenceMeta.columns.push(
                    ...columnsFromTriggerTable.map(columnRef =>
                        columnRef.name
                    )
                );
            }
            else {
                referenceMeta.filters.push( andCondition );
            }
        }

        return referenceMeta;
    }
}