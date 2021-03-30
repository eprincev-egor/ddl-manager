
export class Type {
    readonly value: string;
    constructor(value: string) {
        this.value = value;
    }

    isArray() {
        return /\[\]$/.test(this.value);
    }

    equal(otherType: Type | string) {
        otherType = typeof otherType === "string" ?
            normalize(otherType) :
            normalize(otherType.value);

        const thisType = normalize(this.value);
        return thisType === otherType;
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