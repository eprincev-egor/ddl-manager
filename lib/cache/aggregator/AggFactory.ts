import { FuncCall, Expression, SelectColumn } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";
import { aggregatorsMap } from "./aggregatorsMap";
import { ArrayAgg } from "./ArrayAgg";
import { ColumnNameGenerator } from "./ColumnNameGenerator";
import { StringAgg } from "./StringAgg";

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
        if ( aggCall.name === "string_agg" ) {
            return this.createStringAgg(aggCall);
        }
        else {
            return this.createDefaultAgg(aggCall);
        }
    }

    private createDefaultAgg(aggCall: FuncCall) {

        const aggColumnName = this.columnNameGenerator.generateName(aggCall);
    
        const ConcreteAggregator = aggregatorsMap[ aggCall.name ];
        const agg = new ConcreteAggregator({
            call: aggCall,
            total: Expression.unknown(aggColumnName),
            recalculateSelect: this.updateColumn.recalculateSelect
        });

        return {
            [ aggColumnName ]: agg
        };
    }

    private createStringAgg(aggCall: FuncCall) {
        const map: IAggMap = {};

        const aggColumnName = this.columnNameGenerator.generateName(aggCall);
        const arrayAggColumnName = aggColumnName + "_array_agg";

        const arrayAgg = new ArrayAgg({
            call: new FuncCall(
                "array_agg",
                aggCall.args
            ),
            total: Expression.unknown(arrayAggColumnName),
            recalculateSelect: this.updateColumn.recalculateSelect
        });
    
        const stringAgg = new StringAgg({
            call: aggCall,
            total: Expression.unknown(aggColumnName),
            recalculateSelect: this.updateColumn.recalculateSelect
        }, arrayAgg);

        map[ arrayAggColumnName ] = arrayAgg;
        map[ aggColumnName ] = stringAgg;

        return map;
    }
}
