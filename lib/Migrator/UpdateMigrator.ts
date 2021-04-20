import { AbstractMigrator } from "./AbstractMigrator";
import { Select } from "../ast";
import { TableReference } from "../database/schema/TableReference";
import { IUpdate } from "./Migration";

export class UpdateMigrator extends AbstractMigrator {

    static timeoutOnDeadlock = 5000;

    async drop() {}

    async create() {

        for (const update of this.migration.toCreate.updates) {
            await this.updateCachePackage(update);
        }
    }

    private async updateCachePackage(
        update: IUpdate,
        packageIndex = 0
    ) {
        const limit = 500;
        let needUpdateMore = false;
        const columnsToUpdate = update.select.columns.map(col =>
            col.name
        );

        do {
            // tslint:disable-next-line: no-console
            console.log([
                `updating #${ ++packageIndex }`,
                `table: ${update.forTable}`,
                `columns: ${columnsToUpdate.join(", ")}`,
                `cache: ${update.cacheName} `
            ].join("\n"));

            const updatedCount = await this.tryUpdateCachePackage(
                update.select,
                update.forTable,
                limit
            );
            needUpdateMore = updatedCount >= limit;

            for (const updateAlso of update.recursionWith || []) {
                const columnsToUpdate = update.select.columns.map(col =>
                    col.name
                );

                // tslint:disable-next-line: no-console
                console.log([
                    `recursion updating #${ packageIndex }`,
                    `table: ${updateAlso.forTable}`,
                    `columns: ${columnsToUpdate.join(", ")}`,
                    `cache: ${updateAlso.cacheName} `
                ].join("\n"));

                const updatedAlsoCount = await this.tryUpdateCachePackage(
                    updateAlso.select,
                    updateAlso.forTable,
                    limit
                );

                if ( updatedAlsoCount > 0 ) {
                    needUpdateMore = true;
                }
            }
        } while( needUpdateMore );
    }

    private async tryUpdateCachePackage(
        selectToUpdate: Select,
        forTableRef: TableReference,
        limit: number,
        tryCount = 3
    ): Promise<number> {
        try {
            return await this.postgres.updateCachePackage(
                selectToUpdate,
                forTableRef,
                limit
            );
        } catch(err) {
            if ( /deadlock/.test(err.message) ) {
                if ( tryCount <= 0 ) {
                    throw err;
                }

                await sleep( UpdateMigrator.timeoutOnDeadlock );
                return await this.tryUpdateCachePackage(
                    selectToUpdate,
                    forTableRef,
                    limit,
                    tryCount - 1
                );
            }

            throw err;
        }
    }
}

async function sleep(ms: number) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}