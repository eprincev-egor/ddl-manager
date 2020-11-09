import { FuncCall, SelectColumn, ColumnReference, Expression } from "../../ast";
import assert from "assert";

interface IFuncsByName {
    [funcName: string]: FuncCall[]
};

export class ColumnNameGenerator {
    private readonly updateColumn: SelectColumn;
    private readonly funcsByName: IFuncsByName;

    constructor(updateColumn: SelectColumn) {
        this.updateColumn = updateColumn;
        this.funcsByName = this.groupFuncsByName();
    }

    generateName(aggCall: FuncCall) {
        if ( this.updateColumn.expression.isFuncCall() ) {
            return this.updateColumn.name;
        }

        if ( this.getFuncsByName(aggCall).length === 1 ) {
            return this.updateColumn.name + "_" + aggCall.name;
        }


        if ( this.hasDifferentFiltersAndSameColumn(aggCall) ) {
            return this.generateNameByColumnRefAndFilters(aggCall);
        }
        else {
            return this.generateNameByColumnRef(aggCall);
        }
    }

    private getFuncsByName(aggCall: FuncCall) {
        return this.funcsByName[ aggCall.name ];
    }

    private hasDifferentFiltersAndSameColumn(aggCall: FuncCall) {
        const funcs = this.funcsByName[ aggCall.name ];
        const filterVariants: string[] = [];
        const columnVariants: string[] = [];
    
        for (const funcCall of funcs) {
    
            const filterVariant = funcCall.where ? 
                funcCall.where.toString() : 
                "";
    
            if ( !filterVariants.includes(filterVariant) ) {
                filterVariants.push(filterVariant);
            }

            const columnVariant = this.generateNameByColumnRef(funcCall);
            if ( !columnVariants.includes(columnVariant) ) {
                columnVariants.push(columnVariant);
            }
        }
        
        // TODO: test cases:
        // sum(a) where(x)
        // sum(a) where(x)
        // sum(b) where(x)
        // sum(c) where(x)
        // sum(d) where(y)
        // sum(e) where(y)
        return (
            filterVariants.length > 1 && 
            columnVariants.length === 1
        );
    }

    private generateNameByColumnRef(agg: FuncCall) {
        const firstArg = agg.args[0];
        const potentialColumnRefName = getFirstColumnRefName(firstArg);
        return `${this.updateColumn.name}_${agg.name}_${potentialColumnRefName}`;
    }
    
    private generateNameByColumnRefAndFilters(agg: FuncCall) {
        assert.ok(agg.where);

        const firstArg = agg.args[0];
        const argName = getFirstColumnRefName(firstArg);
        const filterName = getFirstColumnRefName(agg.where);

        return `${this.updateColumn.name}_${agg.name}_${argName}_${filterName}`;
    }
    
    private groupFuncsByName() {
        const funcsByName: IFuncsByName = {};

        for (const funcCall of this.updateColumn.getAggregations()) {
            if ( !funcsByName[ funcCall.name ] ) {
                funcsByName[ funcCall.name ] = [];
            }

            funcsByName[ funcCall.name ].push(funcCall);
        }

        return funcsByName;
    }
}

function getFirstColumnRefName(expression: Expression) {
    const columnReferences = expression.getColumnReferences();
    const firstColumnRef = columnReferences[0] as ColumnReference;
    return firstColumnRef.name;
}
