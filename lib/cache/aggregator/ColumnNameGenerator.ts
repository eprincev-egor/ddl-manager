import { FuncCall, SelectColumn, ColumnReference, Expression } from "../../ast";
import assert from "assert";
import { MAX_NAME_LENGTH } from "../../database/postgres/constants";
import { Database } from "../../database/schema/Database";

interface IFuncsByName {
    [funcName: string]: FuncCall[]
};

export class ColumnNameGenerator {
    private readonly updateColumn: SelectColumn;
    private readonly funcsByName: IFuncsByName;
    private readonly database: Database;

    constructor(database: Database, updateColumn: SelectColumn) {
        this.database = database;
        this.updateColumn = updateColumn;
        this.funcsByName = this.groupFuncsByName();
    }

    generateName(aggCall: FuncCall) {
        const longName = this.generateFullName(aggCall);
        const slicedName = longName.slice(0, MAX_NAME_LENGTH);
        return slicedName;
    }

    private generateFullName(aggCall: FuncCall) {
        if ( this.updateColumn.isAggCall(this.database) ) {
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
        const filterName = getFirstColumnRefName(agg.where as Expression);

        return `${this.updateColumn.name}_${agg.name}_${argName}_${filterName}`;
    }
    
    private groupFuncsByName() {
        const funcsByName: IFuncsByName = {};

        for (const funcCall of this.updateColumn.expression.getFuncCalls()) {
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
