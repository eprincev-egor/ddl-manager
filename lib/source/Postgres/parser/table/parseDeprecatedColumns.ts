import {
    ObjectName
} from "grapeql-lang";

export function parseDeprecatedColumns(deprecatedColumns: ObjectName[]) {
    return deprecatedColumns.map(deprecatedColumnName => 
        deprecatedColumnName.toString()
    );
}
