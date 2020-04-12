import { CreateTable, PrimaryKeyConstraint } from "grapeql-lang";
import { PrimaryKeyDBO } from "../../objects/PrimaryKeyDBO";

export function parsePrimaryKey(
    tableIdentify: string, 
    tableConstraints: CreateTable["row"]["constraints"]
) {
    const gqlPrimaryKey = tableConstraints.find(gqlConstraint => 
        gqlConstraint instanceof PrimaryKeyConstraint
    ) as PrimaryKeyConstraint;

    if ( !gqlPrimaryKey ) {
        return;
    }

    const pgPrimaryKey = new PrimaryKeyDBO({
        table: tableIdentify,
        name: gqlPrimaryKey.get("name").toString(),
        primaryKey: gqlPrimaryKey.get("primaryKey")
            .map(columnName =>
                columnName.toString()
            )
    });

    return pgPrimaryKey;
}
