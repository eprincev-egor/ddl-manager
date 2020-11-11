import { Client } from "pg";

export class PGTypes {
    private db: Client;
    private typeById: {[key: string]: string};
    private loaded: boolean;

    constructor(db: Client) {
        this.db = db;
        this.typeById = {};
        this.loaded = false;
    }

    async load() {
        if ( this.loaded ) {
            return;
        }

        this.typeById = {};

        const result = await this.db.query(`
            select 
                typname, 
                oid, 
                typinput,
                typelem
            from pg_type
        `);
        result.rows.forEach(row => {
            let type = row.typname;

            const isArray = row.typinput === "array_in";
            if ( isArray ) {
                const elemType = result.rows.find(elemRow =>
                    elemRow.oid === row.typelem
                );
                type = elemType.typname + "[]";
            }

            this.typeById[ row.oid ] = type;
        });

        this.loaded = true;
    }

    getTypeById(oid: number | string): string | null {
        const type = this.typeById[ oid ] || null;
        return type;
    }
}