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

            // TODO: try/catch
            updatedCount = await this.postgres.updateCachePackage(
                selectToUpdate,
                forTableRef,
                limit
            );
        } while( updatedCount >= limit );
    }
}