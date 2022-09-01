import { AbstractMigrator } from "./AbstractMigrator";
import { TableReference } from "../database/schema/TableReference";
import { CacheUpdate } from "../Comparator/graph/CacheUpdate";
import { flatMap } from "lodash";

export const packageSize = 20000;
export const parallelPackagesCount = 8;

export class UpdateMigrator extends AbstractMigrator {

    static timeoutOnDeadlock = 1000;

    async drop() {}

    async create() {
        for (const update of this.migration.toCreate.updates) {
            await this.toggleTriggersAndDoUpdate(update);
        }
    }

    private async toggleTriggersAndDoUpdate(update: CacheUpdate) {
        if ( this.migration.needDisableCacheTriggersOnUpdate() ) {
            const cacheTriggers = this.findCacheTriggers(update.table);
            await this.disableTriggers(update.table, cacheTriggers);
    
            await this.doUpdate(update);

            await this.enableTriggers(update.table, cacheTriggers);
        }
        else {
            await this.doUpdate(update);
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

    private findCacheTriggers(onTableRef: TableReference): string[] {
        const onTable = onTableRef.table;
        const table = this.database.getTable(onTable);
        if ( !table ) {
            return []
        }

        const oldTriggers = table.triggers
            .filter(trigger => 
                !this.migration.toDrop.triggers
                    .some(removed => removed.name == trigger.name)
            );
        const newTriggers = this.migration.toCreate.triggers
            .filter(trigger => trigger.table.equal(onTable));

        return [...oldTriggers, ...newTriggers]
            .filter(trigger => trigger.cacheSignature)
            .map(trigger => trigger.name);
    }

    private async disableTriggers(onTableRef: TableReference, triggers: string[]) {
        const onTable = onTableRef.table;
        for (const triggerName of triggers) {
            await this.postgres.disableTrigger(onTable, triggerName);
        }
    }

    private async enableTriggers(onTableRef: TableReference, triggers: string[]) {
        const onTable = onTableRef.table;
        for (const triggerName of triggers) {
            await this.postgres.enableTrigger(onTable, triggerName);
        }
    }

    private async parallelUpdateCacheByIds(update: CacheUpdate) {
        const {min, max} = await this.postgres.selectMinMax(update.table.table);
        if ( min === null || max === null ) {
            return;
        }
        const delta = max - min;

        if ( delta <= packageSize ) {
            await this.tryUpdateCacheRows(
                update, min, max + 1
            );
            return;
        }

        const threadsPromises: Promise<void>[] = [];
        const threadStep = Math.ceil(delta / parallelPackagesCount);
        for (let i = 0; i < parallelPackagesCount; i++) {
            const startId = min + threadStep * i;
            const endId = Math.min(startId + threadStep, max + 1);

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
        while ( startId < endId ) {
            await this.tryUpdateCacheRows(
                update,
                startId,
                startId + packageSize
            );
            startId += packageSize;
        }
    }

    private async tryUpdateCacheRows(
        update: CacheUpdate,
        minId: number,
        maxId: number,
        attemptsNumberAfterDeadlock = 0
    ) {
        logUpdate(update, `parallel updating ids ${minId} - ${maxId}`);

        try {
            await this.postgres.updateCacheForRows(
                update,
                minId, maxId
            );
        } catch(err: any) {
            if ( /deadlock/i.test(err.message) || err.code === "40P01" ) {
                // next attempt must have more timeout 
                const timeoutOnDeadlock = (
                    Math.max(attemptsNumberAfterDeadlock, 5) * 
                    UpdateMigrator.timeoutOnDeadlock
                );
                await sleep( timeoutOnDeadlock );

                await this.tryUpdateCacheRows(
                    update, minId, maxId,
                    attemptsNumberAfterDeadlock + 1
                );
                return;
            }

            throw err;
        }
    }

    private async updateCacheLimitedPackage(
        update: CacheUpdate,
        packageIndex = 0
    ) {
        let needUpdateMore = false;

        do {
            logUpdate(update, `updating #${ ++packageIndex }`);

            const updatedCount = await this.tryUpdateCacheLimitedPackage(update);
            needUpdateMore = updatedCount >= packageSize;

            const updatedAlsoCount = await this.updateAlsoRecursions(
                update, packageIndex
            );
            if ( updatedAlsoCount > 0 ) {
                needUpdateMore = true;
            }
        } while( needUpdateMore );
    }

    private async tryUpdateCacheLimitedPackage(
        update: CacheUpdate,
        attemptsNumberAfterDeadlock = 0
    ): Promise<number> {
        try {
            return await this.postgres.updateCacheLimitedPackage(
                update,
                packageSize
            );
        } catch(err) {
            const message = (err as any).message;
            if ( /deadlock/i.test(message) ) {
                // next attempt must have more timeout 
                const timeoutOnDeadlock = (
                    Math.max(attemptsNumberAfterDeadlock, 5) * 
                    UpdateMigrator.timeoutOnDeadlock
                );
                await sleep( timeoutOnDeadlock );

                return await this.tryUpdateCacheLimitedPackage(
                    update,
                    attemptsNumberAfterDeadlock + 1
                );
            }

            throw err;
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
        logUpdate(updateAlso, `recursion updating #${ packageIndex }`);

        const updatedAlsoCount = await this.tryUpdateCacheLimitedPackage(updateAlso);

        return updatedAlsoCount;
    }
}

async function sleep(ms: number) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

function logUpdate(update: CacheUpdate, theme: string) {
    // tslint:disable-next-line: no-console
    console.log([
        theme,
        `table: ${update.table.table}`,
        `columns: ${flatMap(update.selects, select => select.columns)
                .map(col => col.name).join(", ")}`,
        `cache: ${update.caches.join(", ")} `
    ].join("\n"));
}