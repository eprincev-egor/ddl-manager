import { Cache, Expression } from "../../ast";

export function buildConditionsMatrix(cache: Cache) {
    // top level is "or"
    // second level is "and"
    let conditionsMatrix: Expression[][] = [];

    if ( cache.select.where ) {
        const orConditions = cache.select.where.splitBy("or");

        for (const orCondition of orConditions) {
            const andConditions = orCondition.splitBy("and");

            conditionsMatrix.push(andConditions);
        }
    }

    const onConditionsByTable: {
        [tableName: string]: Expression[][]
    } = {};
    for (const rootFromItem of cache.select.from) {
        for (const join of rootFromItem.joins) {

            const tableName = join.table.table.toString();
            const joinOrConditions = join.on.splitBy("or");

            const tableConditionsMatrix = onConditionsByTable[ tableName ] || [];
            
            for (const joinCondition of joinOrConditions) {
                const joinAndConditions = joinCondition.splitBy("and");
                tableConditionsMatrix.push( joinAndConditions );
            }

            onConditionsByTable[ tableName ] = tableConditionsMatrix;
        }
    }
    
    for (const tableName in onConditionsByTable) {
        const originalTableMatrix = conditionsMatrix.map(line => line.slice());
        conditionsMatrix = [];

        const tableConditionsMatrix = onConditionsByTable[ tableName ];
        for (const andConditions of tableConditionsMatrix) {

            const newMatrix = originalTableMatrix.map(conditions =>
                [...conditions, ...andConditions]
            );
            conditionsMatrix.push(...newMatrix);
        }
    }

    // remove: AND true
    const outputMatrix = conditionsMatrix
        .map(andConditions =>
            andConditions.filter(condition =>
                condition.toString() !== "true"
            )
        )
        .filter(andConditions =>
            andConditions.length > 0
        );
    
    return outputMatrix;
}
