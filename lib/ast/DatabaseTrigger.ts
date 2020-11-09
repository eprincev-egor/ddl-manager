
export interface IDatabaseTriggerParams {
    name: string;
    table: {
        schema: string;
        name: string;
    };
    procedure: {
        schema: string;
        name: string;
        args: string[];
    }

    before?: boolean;
    after?: boolean;
    insert?: boolean;
    delete?: boolean;
    update?: boolean;
    updateOf?: string[];
    when?: string;
    
    constraint?: boolean;
    deferrable?: boolean;
    statement?: boolean;
    initially?: "immediate" | "deferred";

    comment?: string;
}

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

    constructor(json: IDatabaseTriggerParams) {
        Object.assign(this, json);
    }

    getSignature() {
        return `${this.name} on ${ this.table.schema }.${ this.table.name }`;
    }
}