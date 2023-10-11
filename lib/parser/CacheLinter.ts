import { 
    AbstractFromItem,
    AbstractNode,
    ArrayLiteral,
    BinaryOperator,
    ColumnReference, 
    Cursor, 
    EqualAny, 
    FromItemType, 
    FromTable, 
    FunctionCall, 
    Join, 
    Operand,
    Select
} from "psql-lang";
import { CacheSyntax } from "./CacheSyntax";
import { strict } from "assert";

export class CacheLinter {
    static lint(
        cursor: Cursor,
        cache: CacheSyntax
    ) {
        const linter = new CacheLinter(cursor, cache);
        linter.lint();
    }

    private constructor(
        private cursor: Cursor,
        private cache: CacheSyntax,
        private select = cache.row.cache,
        private cacheForItem = new FromTable({row: {
            table: cache.row.for,
            as: cache.row.as
        }})
    ) {}

    private lint() {

        this.validateCte();
        this.validateUnion();
        this.validateGroupBy();
        this.validateFrom();
        this.validateJoins();
        this.validateSubQueries();
        this.validateColumns();
        this.validateColumnsLinks();
        this.validateWhere();
        this.validateStringAggregations();
        this.validateOrderByAndLimit();
    }

    private validateCte() {
        const cte = this.select.row.with
        if ( cte ) {
            this.throwError("CTE (with queries) are not supported", cte);
        }
    }

    private validateUnion() {
        const union = this.select.row.union;
        if ( union ) {
            this.throwError("UNION are not supported", union.select);
        }
    }

    private validateGroupBy() {
        const groupBy = this.select.row.groupBy;
        if ( groupBy?.length ) {
            this.throwError("GROUP BY are not supported", groupBy[0]);
        }
    }

    private validateSubQueries() {
        const subQuery = this.select.filterChildrenByInstance(Select)[0];
        if ( subQuery ) {
            this.throwError("SUB QUERIES are not supported", subQuery);
        }
    }

    private validateColumns() {
        const columns = this.select.row.select ?? [];
        if ( !columns.length ) {
            this.throwError("required select any columns or expressions", this.select);
        }

        const columnsMap: Record<string, boolean> = {};

        for (const columnNode of columns) {
            const columnAlias = columnNode.row.as?.toValue();

            if ( !columnAlias ) {
                this.throwError("required alias for every cache column", columnNode);
            }

            if ( columnAlias in columnsMap ) {
                this.throwError("duplicated cache column", columnNode.row.as!);
            }
            columnsMap[ columnAlias ] = true;
        }
    }

    private validateColumnsLinks() {
        extract(this.select, ColumnReference)
            .forEach(this.validateColumnLink.bind(this));
    }

    private validateColumnLink(columnLink: ColumnReference) {
        const isStar = (
            columnLink.row.allColumns && 
            columnLink.row.column.length === 0
        );
        if ( isStar ) {
            return;
        }

        const sourceFromItem = columnLink.findDeclaration() as FromItemType | undefined;
        const missingSource = (
            !sourceFromItem && 
            !columnLink.isDependentOn(this.cacheForItem)
        )
        if ( missingSource ) {
            if ( columnLink.row.column.length === 1 ) {
                const fromItems = this.select.filterChildrenByInstance(AbstractFromItem);

                if ( fromItems.length != 1 ) {
                    this.throwError(`implicit table reference: ${columnLink}`, columnLink);
                }
                return;
            }
            
            this.throwError(`source for column ${columnLink} not found`, columnLink);
        }
    }

    private validateStringAggregations() {
        this.select.filterChildrenByInstance(FunctionCall)
            .forEach(this.validateStringAgg.bind(this));
    }

    private validateStringAgg(funcCall: FunctionCall) {
        const name = String(funcCall.row.call);
        if ( name != "string_agg" ) {
            return;
        }

        const args = funcCall.row.arguments || [];
        if ( args.length === 1 ) {
            this.throwError("required delimiter for string_agg", funcCall);
        }
    }

    private validateFrom() {
        if ( this.select.row.from.length > 1 ) {
            this.throwError(
                "multiple FROM are not supported",
                this.select.row.from[1]
            )
        }

        const allFromItems = extract(this.select, AbstractFromItem as any);
        for (const fromItem of allFromItems) {
            if ( !(fromItem instanceof FromTable) ) {
                this.throwError("supported only from table", fromItem);
            }
        }
    }

    private validateJoins() {
        this.select.filterChildrenByInstance(Join)
            .forEach(this.validateJoin.bind(this))
    }

    private validateJoin(join: Join) {
        if ( !("on" in join.row) ) {
            this.throwError("required ON condition for join", join);
        }
    }

    private validateWhere() {
        const {from, where} = this.select.row;
        if ( !where || !(from[0] instanceof FromTable) ) {
            return;
        }

        extract(where, EqualAny)
            .forEach(this.validateWhereAny.bind(this));

        extract(where, BinaryOperator)
            .forEach(this.validateWhereArrayIntersection.bind(this));
    }

    private validateWhereAny(node: EqualAny) {
        const isWrongSide = (
            !this.containsFromTable(node.row.operand) // for_table.column = ...
            &&
            !(node.row.equalAny instanceof Select) &&
            this.containsFromTable(node.row.equalAny) // .. =any( from_table.column )
        );
        if ( isWrongSide ) {
            this.throwError([
                "your condition is slow SeqScan, condition should be:",
                `${node.row.equalAny} && ARRAY[ ${ node.row.operand } ]::integer[]`
            ].join("\n"), node);
        }
    }

    private validateWhereArrayIntersection(node: BinaryOperator) {
        if ( node.row.operator !== "&&" ) {
            return;
        }
        const arrayLiteral = getArray(node.row.left, node.row.right);
        if ( !arrayLiteral ) {
            return;
        }

        const operand = getOther(node.row.left, node.row.right, arrayLiteral);

        const isWrongSide = (
            this.containsFromTable(arrayLiteral) && // ... && ARRAY[ from_table.column ]
            !this.containsFromTable(operand) // for_table.column && ...
        );
        if ( isWrongSide ) {
            this.throwError([
                "your condition is slow SeqScan, condition should be:",
                `${arrayLiteral.row.array} = any(${operand})`
            ].join("\n"), node);
        }
    }

    private validateOrderByAndLimit() {
        const orderBy = this.select.row.orderBy;
        const limit = this.select.row.limit;

        if ( !orderBy?.length ) {
            if ( limit ) {
                this.throwError("required ORDER BY", limit);
            }
            return;
        }

        const fromItems = this.select.row.from;
        if ( fromItems.length === 0 ) {
            this.throwError("required FROM ITEM", orderBy[0]);
        }

        const join = extract(this.select, Join)[0];
        if ( join ) {
            this.throwError("joins is not supported for order by/limit 1 trigger", join);
        }
        if ( !limit ) {
            this.throwError("required LIMIT 1", orderBy[0]);
        }
        if ( String(limit) !== "1" ) {
            this.throwError("supported only limit 1", limit);
        }
    }

    private containsFromTable(operand: Operand) {
        const fromItem = this.getFromTable();

        const schemaName = fromItem.row.table.row.schema;
        const tableName = fromItem.row.table.row.name;
        const tableAlias = fromItem.row.as;

        return extract(operand, ColumnReference).some(column => {
            const path = column.row.column;
            if ( path.length === 0 || path.length === 1 ) {
                return true;
            }

            if ( path.length === 2 ) {
                return path[0].equal(tableAlias || tableName);
            }

            return (
                schemaName && 
                path[0].equal(schemaName) &&
                path[1].equal(tableName)
            );
        });
    }

    private getFromTable() {
        const fromItem = this.select.row.from[0];
        strict.ok(fromItem instanceof FromTable, "required from table");
        return fromItem;
    }

    private throwError(
        error: string, 
        atNode: AbstractNode<any>
    ): never {
        this.cursor.throwError(error, atNode);
    }
}

function extract<T extends AbstractNode<any>>(
    node: AbstractNode<any>, 
    NodeClass: new (...args: any[]) => T
): T[] {
    if ( node instanceof NodeClass ) {
        return [node];
    }

    return node.filterChildrenByInstance(NodeClass);
}

function getArray(...nodes: AbstractNode<any>[]): ArrayLiteral | undefined {
    return nodes.find(node => node instanceof ArrayLiteral) as any;
}

function getOther(left: any, right: any, other: any) {
    if ( left === other ) {
        return right;
    }
    return left;
}