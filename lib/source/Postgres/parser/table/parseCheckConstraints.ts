import { CreateTable, CheckConstraint } from "grapeql-lang";
import { CheckConstraintDBO } from "../../objects/CheckConstraintDBO";

export function parseCheckConstraints(
    tableIdentify: string, 
    tableConstraints: CreateTable["row"]["constraints"]
) {
    const gqlCheckConstraints = tableConstraints.filter(gqlConstraint =>
        gqlConstraint instanceof CheckConstraint
    ) as CheckConstraint[];

    return gqlCheckConstraints.map(gqlConstraint =>
        parseCheckConstraint(tableIdentify, gqlConstraint)
    );
}

function parseCheckConstraint(
    tableIdentify: string, 
    gqlConstraint: CheckConstraint
) {
    const pgConstraint = new CheckConstraintDBO({
        table: tableIdentify,
        name: gqlConstraint.get("name").toString(),
        check: gqlConstraint.get("check").toString()
    });

    return pgConstraint;
}