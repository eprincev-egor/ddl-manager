import { 
    TableReference,
    Expression
} from "../../ast";
import { CacheContext } from "../trigger-builder/CacheContext";
import { buildConditionsMatrix } from "./buildConditionsMatrix";
import { findNecessaryTableMatrix } from "./findNecessaryTableMatrix";

export function buildUniversalWhere(context: CacheContext): Expression {
    const necessaryTableMatrix: string[][] = findNecessaryTableMatrix(context);
    const conditionsMatrix: Expression[][] = buildConditionsMatrix(context.cache);

    const linksToChangedTable = context.cache.select.findTableReferences(context.triggerTable);

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
                    context.cache.for.getIdentifier() === sourceName ||
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
                    new TableReference(context.triggerTable)
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
