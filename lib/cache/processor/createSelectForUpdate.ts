import {
    Expression,
    Cache,
    SelectColumn,
    ColumnReference,
    Select,
    FuncCall
} from "../../ast";
import { Database } from "../../database/schema/Database";
import { fromPairs } from "lodash";

export function createSelectForUpdate(
    database: Database,
    cache: Cache
) {
    let {select} = cache;

    select = select.fixArraySearchForDifferentArrayTypes();

    if ( select.orderBy && cache.hasArrayReference(database) ) {
        select = replaceOrderByLimitToArrayAgg(select);
        select = addAggHelperColumns(cache, select);
        return select;
    }

    if ( cache.hasAgg(database) ) {
        return addAggHelperColumns(cache, select);
    }

    if ( select.orderBy ) {
        return addOrderByHelperColumns(cache, select);
    }

    return select;
}

function replaceOrderByLimitToArrayAgg(select: Select) {
    const orderBy = select.getDeterministicOrderBy();
    return select.clone({
        columns: select.columns.map(selectColumn => 
            selectColumn.clone({
                // building expression (like are order by/limit 1):
                // (array_agg( source.column order by source.sort ))[1]
                expression: new Expression([
                    new Expression([
                        new FuncCall("array_agg", [
                            selectColumn.expression
                        ], undefined, false, orderBy),
                    ], true),
                    Expression.unknown("[1]")
                ])
            })
        ),
        orderBy: undefined,
        limit: undefined
    });
}

function addAggHelperColumns(cache: Cache, select: Select) {
    const fromRef = cache.select.getFromTable();
    const from = fromRef.getIdentifier();

    const rowJson = cache.getSourceRowJson();
    const deps = cache.getSourceJsonDeps().map(column =>
        new ColumnReference(fromRef, column)
    );
    const depsMap = fromPairs(deps.map(column =>
        [column.toString(), column]
    ));

    return select.clone({
        columns: [
            ...select.columns,
            new SelectColumn({
                name: cache.jsonColumnName(),
                expression: new Expression([
                    Expression.unknown(`
                        ('{' || string_agg(
                            '"' || ${from}.id::text || '":' || ${rowJson}::text,
                            ','
                        ) || '}')
                    `, depsMap),
                    Expression.unknown("::"),
                    Expression.unknown("jsonb")
                ])
            })
        ]
    });
}

function addOrderByHelperColumns(cache: Cache, select: Select) {
    const helperColumns = getOrderByColumnsRefs(select).map(columnRef =>
        new SelectColumn({
            name: helperColumnName(cache, columnRef.name),
            expression: new Expression([
                new ColumnReference(
                    select.getFromTable(),
                    columnRef.name
                )
            ])
        })
    );

    return select.clone({
        columns: [
            ...select.columns,
            ...helperColumns
        ]
    })
}

export function getOrderByColumnsRefs(select: Select) {
    const orderBy = select.getDeterministicOrderBy()!;
    return orderBy.getColumnReferences();
}

export function helperColumnName(cache: Cache, columnName: string) {
    return `__${cache.name}_${columnName}`;
}
