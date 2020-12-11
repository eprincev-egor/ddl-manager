import { FuncCall, Expression, SelectColumn, Select } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";
import { aggregatorsMap } from "./aggregatorsMap";
import { ArrayAgg } from "./ArrayAgg";
import { ColumnNameGenerator } from "./ColumnNameGenerator";
import { StringAgg } from "./StringAgg";
import { DistinctArrayAgg } from "./DistinctArrayAgg";
import { UniversalAgg } from "./UniversalAgg";

interface IAggMap {
    [column: string]: AbstractAgg;
}

export class AggFactory {

    private updateColumn: SelectColumn;
    private select: Select;
    private columnNameGenerator: ColumnNameGenerator;

    constructor(select: Select, updateColumn: SelectColumn) {
        this.select = select;
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
        
        if ( aggCall.name === "count" ) {
            if ( aggCall.distinct ) {
                return this.createUniversalAgg(aggCall);
            }
            else {
                return this.createDefaultAgg(aggCall);
            }
        }
        else if ( isHardOrderBy(aggCall) ) {
            return this.createUniversalAgg(aggCall);
        }
        else if ( aggCall.name === "string_agg" ) {
            return this.createStringAgg(aggCall);
        }
        else if ( aggCall.name === "array_agg" && aggCall.distinct ) {
            return this.createDistinctArrayAgg(aggCall);
        }
        else {
            return this.createDefaultAgg(aggCall);
        }
    }

    private createDefaultAgg(aggCall: FuncCall) {

        const aggColumnName = this.columnNameGenerator.generateName(aggCall);
    
        const ConcreteAggregator = aggregatorsMap[ aggCall.name ];
        const agg = new ConcreteAggregator({
            select: this.select,
            updateColumn: this.updateColumn,
            call: aggCall,
            total: Expression.unknown(aggColumnName)
        });

        return {
            [ aggColumnName ]: agg
        };
    }

    private createDistinctArrayAgg(aggCall: FuncCall) {
        const map: IAggMap = {};

        const aggColumnName = this.columnNameGenerator.generateName(aggCall);
        const arrayAggAllColumnName = aggColumnName + "_array_agg";

        const arrayAggAll = new ArrayAgg({
            select: this.select,
            updateColumn: this.updateColumn,
            call: new FuncCall(
                "array_agg",
                [aggCall.args[0]],
                aggCall.where,
                false,
                aggCall.orderBy
            ),
            total: Expression.unknown(arrayAggAllColumnName)
        });
    
        const arrayAggDistinct = new DistinctArrayAgg({
            select: this.select,
            updateColumn: this.updateColumn,
            call: aggCall,
            total: Expression.unknown(aggColumnName)
        }, arrayAggAll);

        map[ arrayAggAllColumnName ] = arrayAggAll;
        map[ aggColumnName ] = arrayAggDistinct;

        return map;
    }

    private createStringAgg(aggCall: FuncCall) {
        const map: IAggMap = {};

        const aggColumnName = this.columnNameGenerator.generateName(aggCall);
        const arrayAggColumnName = aggColumnName + "_array_agg";

        const arrayAgg = new ArrayAgg({
            select: this.select,
            updateColumn: this.updateColumn,
            call: new FuncCall(
                "array_agg",
                [aggCall.args[0]]
            ),
            total: Expression.unknown(arrayAggColumnName)
        });
    
        const stringAgg = new StringAgg({
            select: this.select,
            updateColumn: this.updateColumn,
            call: aggCall,
            total: Expression.unknown(aggColumnName)
        }, arrayAgg);

        map[ arrayAggColumnName ] = arrayAgg;
        map[ aggColumnName ] = stringAgg;

        return map;
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
                select: this.select,
                updateColumn: this.updateColumn,
                call: new FuncCall(
                    "array_agg", [
                        new Expression([
                            columnRef
                        ])
                    ]
                ),
                total: Expression.unknown(helperColumnName)
            });
            map[ helperColumnName ] = helperArrayAgg;

            childAggregations.push(helperArrayAgg);
        }

        const universalAgg = new UniversalAgg({
            select: this.select,
            updateColumn: this.updateColumn,
            call: aggCall,
            total: Expression.unknown(aggColumnName)
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
