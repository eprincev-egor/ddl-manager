
export class Type {

    readonly value: string;
    readonly normalized: string;
    constructor(value: string) {
        this.value = value;
        this.normalized = formatType(value) as string;
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

export function formatType(someType?: string) {
    if ( !someType ) {
        return null;
    }

    someType = someType.trim()
        .toLowerCase()
        .replace(/\s+/g, " ")
        .replace(/"/g, "");

    if ( someType.startsWith("numeric") ) {
        return "numeric";
    }
    if ( someType === "float8" ) {
        return "double precision";
    }
    if ( someType === "float8[]" ) {
        return "double precision[]";
    }

    if ( someType === "timestamp" ) {
        return "timestamp without time zone";
    }
    if ( someType === "int8" ) {
        return "bigint";
    }
    if ( someType === "int4" ) {
        return "integer";
    }
    if ( someType === "int2" ) {
        return "smallint";
    }

    if ( someType === "int8[]" ) {
        return "bigint[]";
    }
    if ( someType === "int4[]" ) {
        return "integer[]";
    }
    if ( someType === "int2[]" ) {
        return "smallint[]";
    }
    if ( someType === "bool" ) {
        return "boolean";
    }
    return someType;
}

export function equalType(inputTypeA?: string, inputTypeB?: string) {
    const typeA = formatType(inputTypeA);
    const typeB = formatType(inputTypeB);

    return (
        typeA === typeB ||
        typeA === "public." + typeB ||
        "public." + typeA === typeB
    );
}