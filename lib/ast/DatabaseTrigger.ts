import { DatabaseTriggerType } from "../interface";

export class DatabaseTrigger {
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
}