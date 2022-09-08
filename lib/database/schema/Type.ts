
export class Type {

    readonly value: string;
    readonly normalized: string;
    constructor(value: string) {
        this.value = value;
        this.normalized = normalize(value);
    }

    isArray() {
        return /\[\]$/.test(this.value);
    }

    suit(newType: Type) {
        if ( 
            this.normalized === "bigint" &&
            newType.normalized === "integer" 
        ) {
            return true;
        }

        return this.normalized === newType.normalized;
    }

    toString() {
        return this.value;
    }
}

function normalize(type: string) {
    if ( type === "int8" ) {
        return "bigint";
    }
    if ( type === "int4" ) {
        return "integer";
    }
    if ( type === "int2" ) {
        return "smallint";
    }

    if ( type === "int8[]" ) {
        return "bigint[]";
    }
    if ( type === "int4[]" ) {
        return "integer[]";
    }
    if ( type === "int2[]" ) {
        return "smallint[]";
    }

    if ( type === "bool" ) {
        return "boolean";
    }

    return type;
}