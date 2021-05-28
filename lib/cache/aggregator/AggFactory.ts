import { FuncCall, Expression, SelectColumn } from "../../ast";
import { AbstractAgg } from "./AbstractAgg";
import { aggregatorsMap } from "./aggregatorsMap";
import { ArrayAgg } from "./ArrayAgg";
import { ColumnNameGenerator } from "./ColumnNameGenerator";
import { UniversalAgg } from "./UniversalAgg";
import { StringAgg } from "./StringAgg";
import { MaxAgg } from "./MaxAgg";
import { MinAgg } from "./MinAgg";
import { BoolOrAgg } from "./BoolOrAgg";
import { BoolAndAgg } from "./BoolAndAgg";
import { DistinctArrayAgg } from "./DistinctArrayAgg";
import { Database } from "../../database/schema/Database";
import { MAX_NAME_LENGTH } from "../../database/postgres/constants";

interface IAggMap {
    [column: string]: AbstractAgg;
}

export class AggFactory {

    private database: Database;
    private updateColumn: SelectColumn;
    private columnNameGenerator: ColumnNameGenerator;

    constructor(database: Database, updateColumn: SelectColumn) {
        this.database = database;
        this.updateColumn = updateColumn;
        this.columnNameGenerator = new ColumnNameGenerator(database, updateColumn);
    }

    createAggregations(): IAggMap {
        
        const map: IAggMap = {};

        for (const aggCall of this.updateColumn.getAggregations(this.database)) {
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
            call: aggCall,
            columnName: aggColumnName
        });

        return {
            [ aggColumnName ]: agg
        };
    }

    private createUniversalAgg(aggCall: FuncCall) {
        const map: IAggMap = {};

        const aggColumnName = this.columnNameGenerator.generateName(aggCall);
        const childAggregations: ArrayAgg[] = [];

        const depsColumns = aggCall.withoutWhere().getColumnReferences();
        for (const columnRef of depsColumns) {
            const helperColumnName = (aggColumnName + "_" + columnRef.name)
                .slice(0, MAX_NAME_LENGTH);

            if ( helperColumnName in map ) {
                continue;
            }

            const helperArrayAgg = new ArrayAgg({
                call: new FuncCall(
                    "array_agg", [
                        new Expression([
                            columnRef
                        ])
                    ],
                    aggCall.where
                ),
                columnName: helperColumnName
            });
            map[ helperColumnName ] = helperArrayAgg;

            childAggregations.push(helperArrayAgg);
        }

        let AggConstructor = UniversalAgg;
        if ( aggCall.name === "string_agg" ) {
            AggConstructor = StringAgg;
        }
        else if ( aggCall.name === "max" ) {
            AggConstructor = MaxAgg;
        }
        else if ( aggCall.name === "min" ) {
            AggConstructor = MinAgg;
        }
        else if ( aggCall.name === "bool_or" ) {
            AggConstructor = BoolOrAgg;
        }
        else if ( aggCall.name === "bool_and" || aggCall.name === "every" ) {
            AggConstructor = BoolAndAgg;
        }
        else if (
            aggCall.name === "array_agg" &&
            aggCall.distinct &&
            !aggCall.orderBy 
        ) {
            AggConstructor = DistinctArrayAgg;
        }

        const universalAgg = new AggConstructor({
            call: aggCall,
            columnName: aggColumnName
        }, childAggregations);
        
        map[ aggColumnName ] = universalAgg;

        return map;
    }
}

function isHardOrderBy(aggCall: FuncCall) {
    if ( !aggCall.orderBy ) {
        return false;
    }

    if ( aggCall.orderBy.items.length > 1 ) {
        return true;
    }
    
    const firstArg = aggCall.args[0];
    const firstOrderBy = aggCall.orderBy.items[0].expression;

    const isAlienOrder = firstArg.toString() !== firstOrderBy.toString();
    return isAlienOrder;
}
