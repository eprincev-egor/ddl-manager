import { AbstractComparator } from "./AbstractComparator";
import { Cache } from "../ast/Cache";
import { Index } from "../database/schema/Index";
import { Comment } from "../database/schema/Comment";
import { flatMap } from "lodash";
import { CacheIndex } from "../ast/CacheIndex";

export class IndexComparator extends AbstractComparator {

    drop() {
        const allCacheIndexes = flatMap(
            this.database.tables, 
            table => table.indexes
        ).filter(index => 
            index.comment &&
            index.comment.cacheSignature
        );

        for (const dbIndex of allCacheIndexes) {
            if ( this.existsSameIndexInFS(dbIndex) ) {
                continue;
            }
            
            this.migration.drop({
                indexes: [dbIndex]
            });
        }
    }

    create() {
        for (const file of this.fs.files) {
            this.createNewIndexes( file.content.cache );
        }
    }

    private existsSameIndexInFS(dbIndex: Index): boolean {
        const cachesForThatTable = flatMap(
            this.fs.files,
            file => 
                file.content.cache
        ).filter(cache =>
            cache.for.table.equal(dbIndex.table)
        );

        for (const cache of cachesForThatTable) {
            for (const cacheIndex of cache.indexes) {
                const fsIndex = cacheIndexToDbIndex(cache, cacheIndex);

                if ( fsIndex.equal(dbIndex) ) {
                    return true;
                }
            }
        }
        
        return false;
    }

    private createNewIndexes(caches: Cache[]) {
        for (const cache of caches) {
            const cacheIndexes: CacheIndex[] = cache.indexes || [];
            const indexes: Index[] = cacheIndexes
                .map((cacheIndex) => 
                    cacheIndexToDbIndex(cache, cacheIndex)
                )
                .filter((fsIndex) => 
                    !this.existsSameIndexInDB(fsIndex)
                );

            this.migration.create({
                indexes
            });
        }
    }

    private existsSameIndexInDB(fsIndex: Index) {
        const dbTable = this.database.getTable(fsIndex.table);
        const dbIndexes = dbTable && dbTable.indexes || [];

        for (const dbIndex of dbIndexes) {
            if ( dbIndex.equal(fsIndex) ) {
                return true;
            }
        }

        return false;
    }
}

function cacheIndexToDbIndex(cache: Cache, cacheIndex: CacheIndex): Index {
    return new Index({
        name: `${ cache.for.table.name }_${ cacheIndex.on.join("_") }_cidx`,
        table: cache.for.table,
        index: cacheIndex.index,
        columns: cacheIndex.on.map(elem =>
            elem.toString()
        ),
        comment: Comment.fromFs({
            objectType: "index",
            cacheSignature: cache.getSignature()
        })
    });
}