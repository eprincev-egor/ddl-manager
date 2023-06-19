import {
    Expression,
    Cache,
    SelectColumn,
    ColumnReference,
    Select
} from "../../ast";
import { Database } from "../../database/schema/Database";

export function createSelectForUpdate(
    database: Database,
    cache: Cache
) {
    let selectToUpdate = cache.select;

    if ( cache.hasAgg(database) ) {
        const fromRef = cache.select.getFromTable();
        const from = fromRef.getIdentifier();

        const rowJson = cache.getSourceRowJson();
        const deps = cache.getSourceJsonDeps().map(column =>
            new ColumnReference(fromRef, column)
        );
        const depsMap = Object.fromEntries(deps.map(column =>
            [column.toString(), column]
        ));

        selectToUpdate = selectToUpdate.clone({
            columns: [
                ...selectToUpdate.columns,
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

    if ( cache.select.orderBy ) {
        selectToUpdate = addHelperColumns(cache, selectToUpdate);
    }

    return selectToUpdate;
}

export function addHelperColumns(cache: Cache, select: Select) {
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
    const orderByColumns = select.orderBy!.getColumnReferences();
    
    const hasId = orderByColumns.some(columnRef => columnRef.name === "id");
    if ( !hasId ) {
        orderByColumns.unshift(
            new ColumnReference(
                select.getFromTable(),
                "id"
            )
        );
    }

    return orderByColumns;
}

export function helperColumnName(cache: Cache, columnName: string) {
    return `__${cache.name}_${columnName}`;
}