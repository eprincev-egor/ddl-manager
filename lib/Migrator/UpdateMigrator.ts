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
        let updatedCount = 0;
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

            updatedCount = await this.tryUpdateCachePackage(
                update.select,
                update.forTable,
                limit
            );
        } while( updatedCount >= limit );
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
                    console.log(err.sql);
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