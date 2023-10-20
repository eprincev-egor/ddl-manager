import { Select } from "./Select";
import { FuncCall, ColumnReference, Expression } from "./expression";
import { SelectColumn } from "./SelectColumn";
import { TableReference } from "../database/schema/TableReference";
import { CacheIndex } from "./CacheIndex";
import { findDependenciesTo, findDependenciesToCacheTable } from "../cache/processor/findDependencies";
import { fromPairs } from "lodash";

export class Cache {
    readonly name: string;
    readonly for: TableReference;
    readonly select: Select;
    readonly withoutTriggers: string[];
    readonly withoutInserts: string[];
    readonly indexes: CacheIndex[];
    private selectForUpdate?: Select;

    constructor(
        name: string,
        forTable: TableReference,
        select: Select,
        withoutTriggers: string[] = [],
        withoutInserts: string[] = [],
        indexes: CacheIndex[] = []
    ) {
        this.name = name;
        this.for = forTable;
        this.select = select;
        this.withoutTriggers = withoutTriggers;
        this.withoutInserts = withoutInserts;
        this.indexes = indexes;
    }

    getIsLastColumnName() {
        const helperColumnName = [
            "_",
            this.name,
            "for",
            this.for.table.name
        ].join("_");
        return helperColumnName;
    }

    createSelectForUpdate(aggFunctions: string[]) {
        if ( this.selectForUpdate ) {
            return this.selectForUpdate;
        }

        let {select} = this;
    
        select = select.fixArraySearchForDifferentArrayTypes();
    
        if ( select.orderBy && this.hasArrayReference() ) {
            select = replaceOrderByLimitToArrayAgg(select);
            select = addAggHelperColumns(this, select);
            return select;
        }
    
        if ( this.hasAgg(aggFunctions) ) {
            return addAggHelperColumns(this, select);
        }
    
        if ( select.orderBy ) {
            return addOrderByHelperColumns(this, select);
        }
    
        this.selectForUpdate = select;
        return select;
    }

    equal(otherCache: Cache) {
        return (
            this.name === otherCache.name &&
            this.for.equal(otherCache.for) &&
            this.select.toString() === otherCache.select.toString() &&
            this.withoutTriggers.join(",") === otherCache.withoutTriggers.join(",") &&
            this.withoutInserts.join(",") === otherCache.withoutInserts.join(",") &&
            this.indexes.join(",") === otherCache.indexes.join(",")
        );
    }

    hasForeignTablesDeps() {
        return this.select.from.length > 0;
    }

    getTargetTablesDepsColumns() {
        return findDependenciesToCacheTable(this).columns;
    }

    getFromTable() {
        return this.select.getFromTable().table;
    }

    getFromTableRef() {
        return this.select.getFromTable();
    }

    jsonColumnName() {
        return `__${this.name}_json__`
    }

    getSourceRowJson(recordAlias?: string) {
        const from = this.select.getFromTable().getIdentifier();
        const deps = this.getSourceJsonDeps();

        const maxColumnsPerCall = 50;
        const calls: string[] = [];

        for (let i = 0, n = deps.length; i < n; i += maxColumnsPerCall) {
            const depsPackage = deps.slice(i, i + maxColumnsPerCall);

            calls.push(`jsonb_build_object(
                ${depsPackage.map(column =>
                    `'${column}', ${recordAlias || from}.${column}`
                )}
            )`);
        }

        return calls.join(" || ");
    }

    getSourceJsonDeps() {
        const deps = findDependenciesTo(
            this, this.select.getFromTable(),
            ["id"]
        );
        return deps;
    }

    hasArrayReference() {
        return this.select.hasArraySearchOperator();
    }

    hasAgg(aggFunctions: string[]) {
        return this.select.columns.some(column => 
            column.getAggregations(aggFunctions).length > 0
        );
    }

    getSignature() {
        return `cache ${this.name} for ${this.for}`;
    }

    toString() {
        return `
cache ${this.name} for ${this.for} (
    ${this.select}
)
${ this.withoutTriggers.map(onTable => 
    `without triggers on ${onTable}`
).join(" ").trim() }
${ this.withoutInserts.map(onTable => 
    `without insert case on ${onTable}`
).join(" ").trim() }
${ this.indexes.join("\n") }
        `;
    }
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
