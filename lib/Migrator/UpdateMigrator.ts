import { AbstractMigrator } from "./AbstractMigrator";
import { Select } from "../ast";
import { TableReference } from "../database/schema/TableReference";
import { IUpdate } from "./Migration";
import assert from "assert";

const packageSize = 500;
const parallelPackagesCount = 10;

export class UpdateMigrator extends AbstractMigrator {

    static timeoutOnDeadlock = 5000;

    async drop() {}

    async create() {

        for (const update of this.migration.toCreate.updates) {
            if ( update.isFirst && !update.recursionWith ) {
                await this.parallelUpdateCacheByIds(update);
            }
            else {
                await this.updateCacheLimitedPackage(update);
            }
        }
    }

    private async parallelUpdateCacheByIds(update: IUpdate, offset = 0) {
        assert.ok(update.isFirst, "only first migration can update cache by ids in parallel");

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
        try {
            await this.postgres.updateCacheForRows(
                update.select, update.forTable,
                updateIds
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

            const updatedCount = await this.tryUpdateCacheLimitedPackage(
                update.select,
                update.forTable
            );
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
        selectToUpdate: Select,
        forTableRef: TableReference,
        attemptsNumberAfterDeadlock = 0
    ): Promise<number> {
        try {
            return await this.postgres.updateCacheLimitedPackage(
                selectToUpdate,
                forTableRef,
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
                    selectToUpdate,
                    forTableRef,
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

        for (const updateAlso of update.recursionWith || []) {
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

        const updatedAlsoCount = await this.tryUpdateCacheLimitedPackage(
            updateAlso.select,
            updateAlso.forTable
        );

        return updatedAlsoCount;
    }
}

async function sleep(ms: number) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}