import { Select } from "./Select";
import { TableReference } from "../database/schema/TableReference";
import { CacheIndex } from "./CacheIndex";
import { findDependenciesTo, findDependenciesToCacheTable } from "../cache/processor/findDependencies";
import { Database } from "../database/schema/Database";

export class Cache {
    readonly name: string;
    readonly for: TableReference;
    readonly select: Select;
    readonly withoutTriggers: string[];
    readonly withoutInserts: string[];
    readonly indexes: CacheIndex[];

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

    hasAgg(database: Database) {
        return this.select.columns.some(column => 
            column.getAggregations(database).length > 0
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