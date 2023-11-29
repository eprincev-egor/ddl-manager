import { AbstractMigrator } from "./AbstractMigrator";
import { CacheUpdate } from "../Comparator/graph/CacheUpdate";
import { flatMap } from "lodash";
import { sleep } from "../utils";

export const parallelPackagesCount = 8;

export class UpdateMigrator extends AbstractMigrator {

    static timeoutOnDeadlock = 3000;

    private aborted = false;

    async drop() {}

    async create() {
        for (const update of this.migration.toCreate.updates) {
            await this.tryUpdate(update);
        }
    }

    abort() {
        this.aborted = true;
    }

    private async tryUpdate(update: CacheUpdate) {
        try {
            await this.doUpdate(update);
        } catch(error) {
            this.migration.addLog([
                `[${new Date().toISOString()}]`,
                (error as Error).message
            ].join(" "));
        }
    }

    private async doUpdate(update: CacheUpdate) {
        if ( update.recursionWith.length > 0 ) {
            await this.updateCacheLimitedPackage(update);
        }
        else {
            await this.parallelUpdateCacheByIds(update);
        }
    }

    private async parallelUpdateCacheByIds(update: CacheUpdate) {
        const {min, max} = await this.postgres.selectMinMax(update.table.table);
        if ( min === null || max === null ) {
            return;
        }
        const delta = max - min;

        if ( delta <= this.migration.getUpdatePackageSize() ) {
            await this.tryUpdateCacheRows(
                update, min, max
            );
            return;
        }

        const threadsPromises: Promise<void>[] = [];
        const threadStep = Math.ceil(delta / parallelPackagesCount);
        for (let i = 0; i < parallelPackagesCount; i++) {
            const startId = min + threadStep * i;
            const endId = Math.min(startId + threadStep - 1, max) + 1;

            const threadPromise = this.updateCacheThread(
                update, startId, endId
            );
            threadsPromises.push(threadPromise);
        }
        await Promise.all(threadsPromises);
    }

    private async updateCacheThread(
        update: CacheUpdate,
        startId: number,
        endId: number
    ) {
        const packageSize = this.migration.getUpdatePackageSize();

        while ( startId < endId ) {
            if ( this.aborted ) {
                return;
            }

            let ids = await this.postgres.selectNextIds(
                update.table.table,
                endId,
                packageSize
            );
            ids = ids.filter(id => id >= startId);

            if ( ids.length === 0 ) {
                return;
            }

            await this.tryUpdateCacheRows(
                update,
                ids[0],
                ids[ ids.length - 1 ]
            );
            endId = ids[0];
        }
    }

    private async tryUpdateCacheRows(
        update: CacheUpdate,
        minId: number,
        maxId: number,
        attemptsNumberAfterDeadlock = 0
    ) {
        this.logUpdate(update, `parallel updating ids ${minId} - ${maxId}`);

        const timeStart = new Date();
        if ( this.migration.updateHooks.onStartUpdate ) {
            this.migration.updateHooks.onStartUpdate({
                columns: update.getColumnsRefs(),
                rows: {minId, maxId}
            });
        }

        try {
            await this.postgres.updateCacheForRows(
                update,
                minId, maxId,
                this.migration.getTimeoutPerUpdate()
            );

            if ( this.migration.updateHooks.onUpdate ) {
                const timeEnd = new Date();
                this.migration.updateHooks.onUpdate({
                    columns: update.getColumnsRefs(),
                    rows: {minId, maxId},
                    time: {
                        start: timeStart,
                        end: timeEnd,
                        duration: +timeEnd - +timeStart
                    }
                });
            }
        } catch(err: any) {
            if ( needRepeatUpdate(err, attemptsNumberAfterDeadlock) ) {
                await sleepOnDeadlock( attemptsNumberAfterDeadlock );

                await this.tryUpdateCacheRows(
                    update, minId, maxId,
                    attemptsNumberAfterDeadlock + 1
                );
                return;
            }

            this.logUpdate(update, `failed updating ids ${minId} - ${maxId} with error: ${err.message}`);

            if ( this.migration.updateHooks.onUpdateError ) {
                const timeEnd = new Date();
                this.migration.updateHooks.onUpdateError({
                    columns: update.getColumnsRefs(),
                    error: err,
                    rows: {minId, maxId},
                    time: {
                        start: timeStart,
                        end: timeEnd,
                        duration: +timeEnd - +timeStart
                    }
                });
            }
        }

        const timeout = this.migration.getTimeoutBetweenUpdates();
        if ( timeout ) {
            await sleep(timeout);
        }
    }

    private async updateCacheLimitedPackage(
        update: CacheUpdate,
        packageIndex = 0
    ) {
        let needUpdateMore = false;

        do {
            if ( this.aborted ) {
                return;
            }

            this.logUpdate(update, `updating #${ ++packageIndex }`);

            const updatedCount = await this.tryUpdateCacheLimitedPackage(update);
            needUpdateMore = updatedCount >= this.migration.getUpdatePackageSize();

            const updatedAlsoCount = await this.updateAlsoRecursions(
                update, packageIndex
            );
            if ( updatedAlsoCount > 0 ) {
                needUpdateMore = true;

                const timeout = this.migration.getTimeoutBetweenUpdates();
                if ( timeout ) {
                    await sleep(timeout);
                }
            }
        } while( needUpdateMore );
    }

    private async tryUpdateCacheLimitedPackage(
        update: CacheUpdate,
        attemptsNumberAfterDeadlock = 0
    ): Promise<number> {
        const timeStart = new Date();
        if ( this.migration.updateHooks.onStartUpdate ) {
            this.migration.updateHooks.onStartUpdate({
                columns: update.getColumnsRefs(),
                rows: "package"
            });
        }

        try {
            const updatedIds = await this.postgres.updateCacheLimitedPackage(
                update,
                this.migration.getUpdatePackageSize(),
                this.migration.getTimeoutPerUpdate()
            );

            if ( this.migration.updateHooks.onUpdate ) {
                const timeEnd = new Date();

                this.migration.updateHooks.onUpdate({
                    columns: update.getColumnsRefs(),
                    rows: {
                        minId: Math.min(...updatedIds),
                        maxId: Math.max(...updatedIds)
                    },
                    time: {
                        start: timeStart,
                        end: timeEnd,
                        duration: +timeEnd - +timeStart
                    }
                });
            }

            return updatedIds.length;
        } catch(err: any) {
            if ( needRepeatUpdate(err, attemptsNumberAfterDeadlock) ) {
                await sleepOnDeadlock( attemptsNumberAfterDeadlock );

                return await this.tryUpdateCacheLimitedPackage(
                    update,
                    attemptsNumberAfterDeadlock + 1
                );
            }

            if ( this.migration.updateHooks.onUpdateError ) {
                const timeEnd = new Date();

                this.migration.updateHooks.onUpdateError({
                    columns: update.getColumnsRefs(),
                    error: err,
                    rows: "package",
                    time: {
                        start: timeStart,
                        end: timeEnd,
                        duration: +timeEnd - +timeStart
                    }
                });
            }

            this.logUpdate(update, `failed updating with error: ${err.message}`);
            return 0;
        }
    }

    private async updateAlsoRecursions(
        update: CacheUpdate,
        packageIndex: number
    ) {
        let totalUpdatedCount = 0;

        for (const updateAlso of update.recursionWith) {
            const updatedAlsoCount = await this.updateAlsoRecursion(
                updateAlso, packageIndex
            );

            totalUpdatedCount += updatedAlsoCount;
        }

        return totalUpdatedCount;
    }

    private async updateAlsoRecursion(
        updateAlso: CacheUpdate,
        packageIndex: number
    ) {
        this.logUpdate(updateAlso, `recursion updating #${ packageIndex }`);

        const updatedAlsoCount = await this.tryUpdateCacheLimitedPackage(updateAlso);

        return updatedAlsoCount;
    }

    private logUpdate(update: CacheUpdate, theme: string) {
        this.migration.addLog([
            `[${new Date().toISOString()}]`,
            theme,
            `table: ${update.table.table}`,
            `columns: ${flatMap(update.selects, select => select.columns)
                    .map(col => col.name).join(", ")}`,
            `cache: ${update.caches.join(", ")} `
        ].join("\n"));
    }
}

function needRepeatUpdate(err: any, attemptsCount: number) {
    const isDeadlock = /deadlock/i.test(err.message) || err.code === "40P01";
    const isLongBlock = /terminating connection due to administrator/.test(err.message) || err.code == "57P01";

    return (isDeadlock || isLongBlock) && attemptsCount < 10;
}

async function sleepOnDeadlock(attemptsCount: number) {
    // next attempt must have more timeout 
    const timeoutOnDeadlock = (
        Math.max(attemptsCount, 10) * 
        UpdateMigrator.timeoutOnDeadlock
    );
    await sleep( timeoutOnDeadlock );
}