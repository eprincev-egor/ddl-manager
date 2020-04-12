import { CreateTable, UniqueConstraint } from "grapeql-lang";
import { UniqueConstraintDBO } from "../../objects/UniqueConstraintDBO";

export function parseUniqueConstraints(
    tableIdentify: string, 
    tableConstraints: CreateTable["row"]["constraints"]
) {
    const gqlUniqueConstraints = tableConstraints.filter(gqlConstraint =>
        gqlConstraint instanceof UniqueConstraint
    ) as UniqueConstraint[];

    return gqlUniqueConstraints.map(gqlConstraint =>
        parseUniqueConstraint(tableIdentify, gqlConstraint)
    );
}

function parseUniqueConstraint(
    tableIdentify: string, 
    gqlConstraint: UniqueConstraint
) {
    const pgConstraint = new UniqueConstraintDBO({
        table: tableIdentify,
        name: gqlConstraint.get("name").toString(),
        unique: gqlConstraint.get("unique")
            .map(columnName =>
                columnName.toString()
            )
    });

    return pgConstraint;
}