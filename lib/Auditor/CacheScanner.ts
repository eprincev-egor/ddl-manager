import { CacheColumn } from "../Comparator/graph/CacheColumn";
import { CacheColumnGraph } from "../Comparator/graph/CacheColumnGraph";
import { Cache, Expression, SelectColumn } from "../ast";
import { IDatabaseDriver } from "../database/interface";
import { Database } from "../database/schema/Database";

export interface IFindBrokenColumnsParams {
    timeout?: number;
    concreteTables?: string | string[];
    onStartScanColumn?: (column: string) => void | Promise<void>;
    onScanColumn?: (result: IColumnScanResult) => void | Promise<void>;
    onScanError?: (result: IColumnScanError) => void | Promise<void>;
    onFinish?: () => void | Promise<void>;
}

export interface IColumnScanResult {
    column: string;
    hasWrongValues: boolean;
    wrongExample?: {
        actual: any;
        expected: any;
        table: string;
        selectExpectedForThatRow: string;
        row: Record<string, any>;
        sourceRows?: Record<string, any>[];
    };
    time: TimeRange;
}

export interface IColumnScanError {
    column: string;
    error: Error;
    time: TimeRange;
}

export interface TimeRange {
    start: Date;
    end: Date;
    duration: number;
}

export class CacheScanner {

    constructor(
        private driver: IDatabaseDriver,
        private database: Database,
        private graph: CacheColumnGraph
    ) {}

    async scan(params: IFindBrokenColumnsParams = {}) {
        let allCacheColumns = this.graph.findCacheColumnsForTablesOrColumns(params.concreteTables);

        const brokenColumns: CacheColumn[] = [];

        for (const column of allCacheColumns) {
            const hasWrongValues = await this.tryScanColumnOnWrongValues(params, column);

            if ( hasWrongValues ) {
                brokenColumns.push(column);
            }
        }
        
        if ( params.onFinish ) {
            await params.onFinish();
        }

        return brokenColumns;
    }


    private async tryScanColumnOnWrongValues(
        params: IFindBrokenColumnsParams,
        column: CacheColumn
    ) {
        const timeStart = new Date();

        if ( params.onStartScanColumn ) {
            await params.onStartScanColumn(column.toString());
        }

        try {
            const wrongExample = await this.tryFindWrongRowExampleForColumn(params, column);
            
            if ( params.onScanColumn ) {
                const timeEnd = new Date();

                await params.onScanColumn({
                    column: column.getId(),
                    hasWrongValues: !!wrongExample,
                    wrongExample,
                    time: {
                        start: timeStart,
                        end: timeEnd,
                        duration: +timeEnd - +timeStart
                    }
                });
            }

            return wrongExample;
        } catch(error) {
            if ( params.onScanError ) {
                const timeEnd = new Date();

                await params.onScanError({
                    column: column.toString(),
                    error: error as any,
                    time: {
                        start: timeStart,
                        end: timeEnd,
                        duration: +timeEnd - +timeStart
                    }
                });
            }
        }
    }

    private async tryFindWrongRowExampleForColumn(
        params: IFindBrokenColumnsParams,
        column: CacheColumn
    ) {
        const columnRef = `${column.for.getIdentifier()}.${column.name}`;
        
        let details = "null::json";
        if ( column.select.from.length === 1 ) {
            const selectSql = column.select.toString();
            const fromAlias = column.select.getFromTable().getIdentifier();
            const sourceRowJson = `row_to_json(${fromAlias}.*)`;

            if ( this.database.aggregators.some(aggName => 
                selectSql.includes(aggName + "("))
            ) {
                details = `array_agg(${sourceRowJson})`;
            }
            else if ( column.select.orderBy ) {
                details = `(${column.select.clone({
                    columns: [new SelectColumn({
                        expression: Expression.unknown(`array_agg(${sourceRowJson})`),
                        name: "source_row"
                    })],
                    orderBy: undefined,
                    limit: undefined
                })})`;
            }
            else {
                details = `ARRAY[${sourceRowJson}]`;
            }
        }

        const selectExpected = column.select.clone({
            columns: [
                ...column.select.columns,
                new SelectColumn({
                    expression: Expression.unknown(details),
                    name: "__cache_source_row_details__"
                })
            ]
        });        
        let whereBroken = `${columnRef} is distinct from tmp.${column.name}`;

        const expression = column.getColumnExpression();
        if ( expression.isFuncCall() ) {
            const [call] = expression.getFuncCalls();
            if ( call.name === "array_agg" ) {
                whereBroken = `
                    ${columnRef} is distinct from tmp.${column.name} and
                    not(${columnRef} @> tmp.${column.name} and
                    ${columnRef} <@ tmp.${column.name})
                `
            }

            if ( call.name === "sum" ) {
                whereBroken = `coalesce(${columnRef}, 0) is distinct from coalesce(tmp.${column.name}, 0)`
            }

            if ( call.name === "string_agg" && !call.orderBy ) {
                const actual = `string_to_array(coalesce(${columnRef}, ''), ${call.args[1]})`;
                const expected = `string_to_array(coalesce(tmp.${column.name}, ''), ${call.args[1]})`;
                whereBroken = `
                    ${actual} is distinct from ${expected} and
                    not(${actual} @> ${expected} and
                    ${actual} <@ ${expected})
                `;
            }
        }

        if ( column.name == Cache.generateJsonHelperColumnName(column.cache.name) ) {
            whereBroken = `coalesce(${columnRef}, '{}'::jsonb) is distinct from coalesce(tmp.${column.name}, '{}'::jsonb)`;
        }

        const selectHasBroken = `
            select
                ${columnRef} as "actual",
                tmp.${column.name} as "expected",
                '${column.getTableId()}' as "table",
                row_to_json(${column.for.getIdentifier()}.*) as "row",
                tmp.__cache_source_row_details__ as "sourceRows"
            from ${column.for}
            
            left join lateral (
                ${selectExpected.toSQL()}
            ) as tmp on true
            
            where
                ${whereBroken}

            order by ${column.for.getIdentifier()}.id
            limit 1
        `;

        const {rows} = await this.driver.queryWithTimeout(
            selectHasBroken,
            params.timeout ?? 0
        );

        const wrongExample = rows[0] as IColumnScanResult["wrongExample"];
        if ( wrongExample ) {
            wrongExample.table = column.getTableId();
            wrongExample.selectExpectedForThatRow = `
                select
                    ${column.for.getIdentifier()}.id,
                    ${columnRef} as "actual",
                    tmp.${column.name} as "expected"
                from ${column.for}
                
                left join lateral (
                    ${selectExpected.toSQL()}
                ) as tmp on true
                
                where
                    ${column.for.getIdentifier()}.id = ${wrongExample.row.id} and
                    ${columnRef} is distinct from tmp.${column.name}
            `;
        }

        return wrongExample;
    }

}