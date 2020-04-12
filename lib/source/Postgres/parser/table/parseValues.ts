import { ValuesRow } from "grapeql-lang";

export function parseValues(tableValues: ValuesRow[]): string[][] {
    const pgValues = tableValues.map(valuesRow => 
        valuesRow.get("values").map(valueItem =>
            valueItem.toString()
        )
    );
    return pgValues;
}