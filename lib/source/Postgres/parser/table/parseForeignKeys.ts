import { CreateTable, ForeignKeyConstraint } from "grapeql-lang";
import { ForeignKeyDBO } from "../../objects/ForeignKeyDBO";

export function parseForeignKeys(
    tableIdentify: string, 
    tableConstraints: CreateTable["row"]["constraints"]
) {
    const gqlForeignKeys = tableConstraints.filter(gqlConstraint =>
        gqlConstraint instanceof ForeignKeyConstraint
    ) as ForeignKeyConstraint[];

    return gqlForeignKeys.map(gqlConstraint =>
        parseForeignKey(tableIdentify, gqlConstraint)
    );
}

function parseForeignKey(
    tableIdentify: string, 
    gqlConstraint: ForeignKeyConstraint
) {
    const pgConstraint = new ForeignKeyDBO({
        table: tableIdentify,
        name: gqlConstraint.get("name").toString(),
        
        columns: toStringArr( gqlConstraint.get("columns") ),
        referenceTable: gqlConstraint.get("referenceTable").toString(),
        referenceColumns: toStringArr( gqlConstraint.get("referenceColumns") ),
        
        match: gqlConstraint.get("match"),
        onDelete: gqlConstraint.get("onDelete"),
        onUpdate: gqlConstraint.get("onUpdate")
    });

    return pgConstraint;
}

function toStringArr(syntaxArr: {toString(): string}[]) {
    return syntaxArr.map(syntax => syntax.toString());
}