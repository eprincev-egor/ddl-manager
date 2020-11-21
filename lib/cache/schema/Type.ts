
export class Type {
    readonly value: string;
    constructor(value: string) {
        this.value = value;
    }

    isArray() {
        return /\[\]$/.test(this.value);
    }
}