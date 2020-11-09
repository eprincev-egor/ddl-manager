import { 
    TableReference,
    Table,
    Expression,
    Cache
} from "../../ast";
import { buildConditionsMatrix } from "./buildConditionsMatrix";
import { findNecessaryTableMatrix } from "./findNecessaryTableMatrix";

export function buildFromAndWhere(cache: Cache, triggerTable: Table) {
    const linksToChangedTable = cache.select.findTableReferences(triggerTable);
    if ( !linksToChangedTable.length ) {
        throw new Error(`no ${triggerTable.toString()} in select`);
    }

    const conditionsMatrix: Expression[][] = buildConditionsMatrix(cache);
    const necessaryTableMatrix: string[][] = findNecessaryTableMatrix(
        cache,
        triggerTable,
        conditionsMatrix
    );

    const from = buildFrom(
        cache,
        triggerTable,
        necessaryTableMatrix
    );
    const where = buildWhere(
        cache,
        triggerTable,
        conditionsMatrix,
        necessaryTableMatrix
    );

    return {
        from,
        where
    };
}

function buildFrom(
    cache: Cache,
    triggerTable: Table,
    necessaryTableMatrix: string[][]
) {
    const fromItems: string[] = [triggerTable.toStringWithoutPublic()];

    for (const necessaryTableAliases of necessaryTableMatrix) {
        for (const identifier of necessaryTableAliases) {

            const refFilter = TableReference.identifier2filter(identifier);

            const tableReference = cache.select.findTableReference(refFilter);
            if ( !tableReference ) {
                continue;
            }
            
            if ( fromItems.includes(tableReference.toString()) ) {
                continue;
            }
            fromItems.push(tableReference.toString());
        }
    }

    return fromItems;
}

function buildWhere(
    cache: Cache,
    triggerTable: Table,
    conditionsMatrix: Expression[][],
    necessaryTableMatrix: string[][]
): Expression {
    const linksToChangedTable = cache.select.findTableReferences(triggerTable);

    const orConditions: Expression[] = [];

    for (let i = 0, n = conditionsMatrix.length; i < n; i++) {
        const conditions = conditionsMatrix[i];
        const necessaryTableAliases = necessaryTableMatrix[i];
        
        const changedTableIdentifiers = linksToChangedTable.map(linkClause =>
            linkClause.getIdentifier()
        );

        const andConditions: Expression[] = [];

        for (const condition of conditions) {

            const sourceNames = condition.getColumnReferences()
                .map(columnRef =>
                    columnRef.tableReference.getIdentifier()
                );
            
            const containOnlyNecessaryTables = (
                sourceNames.every(sourceName =>
                    changedTableIdentifiers.includes(sourceName) ||
                    cache.for.getIdentifier() === sourceName ||
                    necessaryTableAliases.includes( sourceName )
                )
            );

            if ( !containOnlyNecessaryTables ) {
                continue;
            }

            let actualCondition = condition;
            for (const changedTableClause of linksToChangedTable) {
                actualCondition = actualCondition.replaceTable(
                    changedTableClause,
                    new TableReference(triggerTable)
                );
            }

            andConditions.push(
                actualCondition
            );
        }

        const andSql = Expression.and(andConditions);
        const andConditionAlreadyExists = orConditions.some(someCondition =>
            andSql.equal(someCondition)
        );
        if ( andConditionAlreadyExists ) {
            continue;
        }
        orConditions.push(andSql);
    }

    return Expression.or(orConditions);
}
