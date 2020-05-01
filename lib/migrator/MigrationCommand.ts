import { IDBO } from "../common";

export interface IMigrationCommandParams {
    type: "drop" | "create";
    object: IDBO;
}

export class MigrationCommand {
    type: IMigrationCommandParams["type"];
    object: IDBO;

    constructor(params: IMigrationCommandParams) {
        this.type = params.type;
        this.object = params.object;
    }
}