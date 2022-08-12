import { AbstractMigrator } from "./AbstractMigrator";
import { IUpdate } from "./Migration";
import { TableID } from "../database/schema/TableID";

export const packageSize = 5000;
export const parallelPackagesCount = 8;

export class UpdateMigrator extends AbstractMigrator {

    static timeoutOnDeadlock = 1000;

    async drop() {}

    async create() {
        for (const update of this.migration.toCreate.updates) {
            const updatingTable = update.forTable.table;
            const triggers = this.findTriggers(updatingTable);

            await this.disableTriggers(updatingTable, triggers);

            if ( update.recursionWith.length > 0 ) {
                await this.updateCacheLimitedPackage(update);
            }
            else {
                await this.parallelUpdateCacheByIds(update);
            }

            await this.enableTriggers(updatingTable, triggers);
        }
    }

    private findTriggers(onTable: TableID): string[] {
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
            .filter(trigger => !trigger.cacheSignature)
            .filter(trigger => trigger.update || trigger.updateOf)
            .map(trigger => trigger.name);
    }

    private async disableTriggers(onTable: TableID, triggers: string[]) {
        for (const triggerName of triggers) {
            await this.postgres.disableTrigger(onTable, triggerName);
        }
    }

    private async enableTriggers(onTable: TableID, triggers: string[]) {
        for (const triggerName of triggers) {
            await this.postgres.enableTrigger(onTable, triggerName);
        }
    }

    private async parallelUpdateCacheByIds(update: IUpdate) {
        const {min, max} = await this.postgres.selectMinMax(update.forTable.table);
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
        update: IUpdate,
        startId: number,
        endId: number
    ) {
        while ( startId < endId ) {
            await this.tryUpdateCacheRows(update, startId, endId);
            startId += packageSize;
        }
    }

    private async tryUpdateCacheRows(
        update: IUpdate,
        minId: number,
        maxId: number,
        attemptsNumberAfterDeadlock = 0
    ) {
        // tslint:disable-next-line: no-console
        console.log([
            `parallel updating ids ${minId} - ${maxId}`,
            `table: ${update.forTable}`,
            `columns: ${update.select.columns.map(col => col.name).join(", ")}`,
            `cache: ${update.cacheName} `
        ].join("\n"));

        try {
            await this.postgres.updateCacheForRows(
                update.select, update.forTable,
                minId, maxId,
                update.cacheName
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
        update: IUpdate,
        packageIndex = 0
    ) {
        let needUpdateMore = false;

        do {
            // tslint:disable-next-line: no-console
            console.log([
                `updating #${ ++packageIndex }`,
                `table: ${update.forTable}`,
                `columns: ${update.select.columns.map(col => col.name).join(", ")}`,
                `cache: ${update.cacheName} `
            ].join("\n"));

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
        update: IUpdate,
        attemptsNumberAfterDeadlock = 0
    ): Promise<number> {
        try {
            return await this.postgres.updateCacheLimitedPackage(
                update.select,
                update.forTable,
                packageSize,
                update.cacheName
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
        update: IUpdate,
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
        updateAlso: IUpdate,
        packageIndex: number
    ) {

        // tslint:disable-next-line: no-console
        console.log([
            `recursion updating #${ packageIndex }`,
            `table: ${updateAlso.forTable}`,
            `columns: ${updateAlso.select.columns.map(col => col.name).join(", ")}`,
            `cache: ${updateAlso.cacheName} `
        ].join("\n"));

        const updatedAlsoCount = await this.tryUpdateCacheLimitedPackage(updateAlso);

        return updatedAlsoCount;
    }
}

async function sleep(ms: number) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}