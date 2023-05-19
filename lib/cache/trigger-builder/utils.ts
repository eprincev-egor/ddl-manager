export function leadingZero(inputNumber: number, length: number): string {
    let output = String(inputNumber);

    for (let i = output.length; i < length; i++) {
        output = "0" + output;
    }

    return output;
}