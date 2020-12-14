import { AbstractMigrator } from "./AbstractMigrator";
import { Select } from "../ast";
import { TableReference } from "../database/schema/TableReference";

export class UpdateMigrator extends AbstractMigrator {
    async drop() {}

    async create() {

        for (const update of this.migration.toCreate.updates) {
            await this.updateCachePackage(
                update.select,
                update.forTable
            );
        }
    }

    private async updateCachePackage(
        selectToUpdate: Select,
        forTableRef: TableReference,
        packageIndex = 0
    ) {
        const limit = 500;
        let updatedCount = 0;

        do {
            // tslint:disable-next-line: no-console
            console.log(`updating ${forTableRef} #${ ++packageIndex }`);

            updatedCount = await this.tryUpdateCachePackage(
                selectToUpdate,
                forTableRef,
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
            // error can be are deadlock
            if ( tryCount <= 0 ) {
                return 0;
            }

            await sleep(5000);
            return await this.tryUpdateCachePackage(
                selectToUpdate,
                forTableRef,
                limit,
                tryCount - 1
            );
        }
    }
}

async function sleep(ms: number) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}