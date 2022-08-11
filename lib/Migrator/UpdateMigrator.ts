import { AbstractMigrator } from "./AbstractMigrator";
import { IUpdate } from "./Migration";
import { TableID } from "../database/schema/TableID";

export const packageSize = 1000;
export const parallelPackagesCount = 16;

export class UpdateMigrator extends AbstractMigrator {

    static timeoutOnDeadlock = 5000;

    async drop() {}

    async create() {
        for (const update of this.migration.toCreate.updates) {
            const updatingTable = update.forTable.table;
            const notCacheTriggers = this.findNotCacheTriggers(updatingTable);

            await this.disableTriggers(updatingTable, notCacheTriggers);

            if ( update.recursionWith.length > 0 ) {
                await this.updateCacheLimitedPackage(update);
            }
            else {
                await this.parallelUpdateCacheByIds(update);
            }

            await this.enableTriggers(updatingTable, notCacheTriggers);
        }
    }

    private findNotCacheTriggers(onTable: TableID): string[] {
        const table = this.database.getTable(onTable);
        if ( !table ) {
            return []
        }

        return table.triggers
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

    private async parallelUpdateCacheByIds(update: IUpdate, offset = 0) {
        const limit = packageSize * parallelPackagesCount;
        const updateIds = await this.postgres.selectIds(
            update.forTable.table,
            offset, limit
        );
        if ( !updateIds.length ) {
            return;
        }

        const packageUpdates: Promise<void>[] = [];
        for (let i = 0; i < Math.min(limit, updateIds.length); i += packageSize) {

            const packageIds = updateIds.slice(i, i + packageSize);
            const updatePromise = this.tryUpdateCacheRows(
                update, packageIds
            );
            packageUpdates.push(updatePromise);
        }
        await Promise.all(packageUpdates);

        await this.parallelUpdateCacheByIds(
            update, offset + limit
        );
    }

    private async tryUpdateCacheRows(
        update: IUpdate, updateIds: number[],
        attemptsNumberAfterDeadlock = 0
    ) {
        // tslint:disable-next-line: no-console
        console.log([
            `parallel updating ids ${ updateIds[0] } - ${ updateIds.slice(-1) }`,
            `table: ${update.forTable}`,
            `columns: ${update.select.columns.map(col => col.name).join(", ")}`,
            `cache: ${update.cacheName} `
        ].join("\n"));

        try {
            await this.postgres.updateCacheForRows(
                update.select, update.forTable,
                updateIds, update.cacheName
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
                    update, updateIds,
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