import { DatabaseTriggerType } from "../interface";

export class DatabaseTrigger {
    name!: string;
    frozen?: boolean;
    comment?: string;
    procedure!: {
        schema: string;
        name: string;
        args: string[];
    };
    table!: {
        schema: string;
        name: string;
    };

    constructor(json: DatabaseTriggerType) {
        Object.assign(this, json);
    }

    getSignature() {
        return `${this.name} on ${ this.table.schema }.${ this.table.name }`;
    }
}