import { FuncCall, Expression, SelectColumn, ColumnReference, UnknownExpressionElement } from "../../ast";
import { TableID } from "../../database/schema/TableID";
import { TableReference } from "../../database/schema/TableReference";
import { AbstractAgg } from "./AbstractAgg";
import { aggregatorsMap } from "./aggregatorsMap";
import { ArrayAgg } from "./ArrayAgg";
import { ColumnNameGenerator } from "./ColumnNameGenerator";
import { UniversalAgg } from "./UniversalAgg";

interface IAggMap {
    [column: string]: AbstractAgg;
}

export class AggFactory {

    private updateColumn: SelectColumn;
    private columnNameGenerator: ColumnNameGenerator;

    constructor(updateColumn: SelectColumn) {
        this.updateColumn = updateColumn;
        this.columnNameGenerator = new ColumnNameGenerator(updateColumn);
    }

    createAggregations(): IAggMap {
        
        const map: IAggMap = {};

        for (const aggCall of this.updateColumn.getAggregations()) {
            const subMap = this.createAggregationsByAggCall(aggCall);
            Object.assign(map, subMap);
        }

        return map;
    }

    private createAggregationsByAggCall(aggCall: FuncCall) {
        const isSimpleOrderBy = !isHardOrderBy(aggCall);
        
        if ( aggCall.name === "count" && !aggCall.distinct ) {
            return this.createSimpleAgg(aggCall);
        }

        if ( aggCall.name === "sum" && !aggCall.distinct && isSimpleOrderBy ) {
            return this.createSimpleAgg(aggCall);
        }

        if ( aggCall.name === "array_agg" && !aggCall.distinct && isSimpleOrderBy ) {
            return this.createSimpleAgg(aggCall);
        }

        return this.createUniversalAgg(aggCall);
    }

    private createSimpleAgg(aggCall: FuncCall) {

        const aggColumnName = this.columnNameGenerator.generateName(aggCall);
    
        const ConcreteAggregator = aggregatorsMap[ aggCall.name ];
        const agg = new ConcreteAggregator({
            updateColumn: this.updateColumn,
            call: aggCall,
            total: UnknownExpressionElement.fromSql(aggColumnName, {
                [aggColumnName]: new ColumnReference(
                    new TableReference(new TableID(
                        "",
                        ""
                    )),
                    aggColumnName
                )
            })
        });

        return {
            [ aggColumnName ]: agg
        };
    }

    private createUniversalAgg(aggCall: FuncCall) {
        const map: IAggMap = {};

        const aggColumnName = this.columnNameGenerator.generateName(aggCall);
        const childAggregations: ArrayAgg[] = [];

        const depsColumns = aggCall.getColumnReferences();
        for (const columnRef of depsColumns) {
            const helperColumnName = aggColumnName + "_" + columnRef.name;

            if ( helperColumnName in map ) {
                continue;
            }

            const helperArrayAgg = new ArrayAgg({
                updateColumn: this.updateColumn,
                call: new FuncCall(
                    "array_agg", [
                        new Expression([
                            columnRef
                        ])
                    ],
                    aggCall.where
                ),
                total: UnknownExpressionElement.fromSql(helperColumnName, {
                    [helperColumnName]: new ColumnReference(
                        new TableReference(new TableID(
                            "",
                            ""
                        )),
                        helperColumnName
                    )
                })
            });
            map[ helperColumnName ] = helperArrayAgg;

            childAggregations.push(helperArrayAgg);
        }

        const universalAgg = new UniversalAgg({
            updateColumn: this.updateColumn,
            call: aggCall,
            total: UnknownExpressionElement.fromSql(aggColumnName)
        }, childAggregations);
        
        map[ aggColumnName ] = universalAgg;

        return map;
    }
}

function isHardOrderBy(aggCall: FuncCall) {
    if ( aggCall.orderBy.length === 0 ) {
        return false;
    }

    if ( aggCall.orderBy.length > 1 ) {
        return true;
    }
    
    const firstArg = aggCall.args[0];
    const firstOrderBy = aggCall.orderBy[0].expression;

    const isAlienOrder = firstArg.toString() !== firstOrderBy.toString();
    return isAlienOrder;
}
