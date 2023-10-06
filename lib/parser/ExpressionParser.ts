import {
    Sql,
    Expression as ExpressionSyntax,
    FunctionCall,
    ColumnReference as ColumnLink,
    CaseWhen as CaseWhenSyntax,
    ArrayLiteral as PgArray,
    Extract as ExtractSyntax,
    Operand,
    BinaryOperator, PreUnaryOperator, PostUnaryOperator, 
    SubExpression,
    CastTo,
    SquareBrackets,
    EqualAny, In
} from "psql-lang";
import { 
    IExpressionElement,
    FuncCall,
    Select,
    Operator,
    CaseWhen,
    Expression,
    UnknownExpressionElement,
    OrderByItem,
    OrderBy,
    Extract
} from "../ast";
import { UnknownExpressionElementParser } from "./UnknownExpressionElementParser";
import { ColumnReferenceParser } from "./ColumnReferenceParser";
import { TableReference } from "../database/schema/TableReference";
import { ArrayElement } from "../ast/expression/ArrayElement";
import {strict} from "assert";

const funcsWithoutOrderBy = [
    // 1 + 1 = 1 + 1
    "count",
    // a + b = b + a
    "sum",
    // Math.max(a, b) = Math.max(b, a)
    "max",
    // Math.min(a, b) = Math.max(b, a)
    "min",
    // x || y = y || x
    "bool_or",
    // x && y = y && x
    "bool_and"
];

export class ExpressionParser {

    private columnReferenceParser = new ColumnReferenceParser();
    private unknownExpressionElementParser = new UnknownExpressionElementParser();

    parse(
        select: Select,
        additionalTableReferences: TableReference[],
        input: string | ExpressionSyntax | Operand
    ): Expression {
        const sql = toOperand(input);

        const elements = this.parseElements(select, additionalTableReferences, sql);
        if ( elements.length === 1 && elements[0] instanceof Expression ) {
            return elements[0] as Expression;
        }

        return new Expression(elements);
    }

    private parseElements(
        select: Select,
        additionalTableReferences: TableReference[],
        elemSyntax: any
    ): IExpressionElement[] {
        if ( elemSyntax instanceof SubExpression ) {
            return [new Expression(
                this.parseElements(
                    select, additionalTableReferences,
                    elemSyntax.row.subExpression
                ), true
            )];
        }

        return this.tryParseBinaryOperator(select, additionalTableReferences, elemSyntax) || [
            this.tryParseFunctionCall(select, additionalTableReferences, elemSyntax) ||
            this.tryParsePgArray(select, additionalTableReferences, elemSyntax) ||
            this.tryParseExtract(select, additionalTableReferences, elemSyntax) ||
            this.tryParseCaseWhen(select, additionalTableReferences, elemSyntax) ||
            this.tryParseColumnRef(select, additionalTableReferences, elemSyntax) ||
            this.parseUnknown(select, additionalTableReferences, elemSyntax)
        ];
    }

    private tryParseBinaryOperator(
        select: Select,
        additionalTableReferences: TableReference[],
        elemSyntax: any
    ) {
        if ( elemSyntax instanceof BinaryOperator ) {
            return [
                ...this.parseElements(
                    select, additionalTableReferences,
                    elemSyntax.row.left
                ),
                new Operator(
                    elemSyntax.row.operator
                ),
                ...this.parseElements(
                    select, additionalTableReferences,
                    elemSyntax.row.right
                )
            ];
        }

        if ( elemSyntax instanceof PreUnaryOperator ) {
            return [
                new Operator(
                    elemSyntax.row.preOperator
                ),
                ...this.parseElements(
                    select, additionalTableReferences,
                    elemSyntax.row.operand
                )
            ];
        }

        if ( elemSyntax instanceof PostUnaryOperator ) {
            return [
                ...this.parseElements(
                    select, additionalTableReferences,
                    elemSyntax.row.operand
                ),
                new Operator(
                    elemSyntax.row.postOperator
                )
            ];
        }

        if ( elemSyntax instanceof CastTo ) {
            return [
                ...this.parseElements(
                    select, additionalTableReferences,
                    elemSyntax.row.cast
                ),
                new Operator("::"),
                UnknownExpressionElement.fromSql(elemSyntax.row.to.toString())
            ];
        }

        if ( elemSyntax instanceof SquareBrackets ) {
            return [
                ...this.parseElements(
                    select, additionalTableReferences,
                    elemSyntax.row.operand
                ),
                UnknownExpressionElement.fromSql(
                    "[" + elemSyntax.row.index.toString() + "]"
                )
            ];
        }

        if ( elemSyntax instanceof EqualAny ) {
            return [
                ...this.parseElements(
                    select, additionalTableReferences,
                    elemSyntax.row.operand
                ),
                new Operator("="),
                UnknownExpressionElement.fromSql(
                    "any(" + elemSyntax.row.equalAny.toString() + ")",
                    this.unknownExpressionElementParser.buildColumnsMap(
                        select, additionalTableReferences,
                        elemSyntax.row.equalAny
                    )
                )
            ];
        }

        if ( elemSyntax instanceof In ) {
            const unknownSql = Array.isArray(elemSyntax.row.in) ?
                `in (${elemSyntax.row.in.join(", ")})` :
                `in (${elemSyntax.row.in})`;

            return [
                ...this.parseElements(
                    select, additionalTableReferences,
                    elemSyntax.row.operand
                ),
                UnknownExpressionElement.fromSql(
                    unknownSql,
                    this.unknownExpressionElementParser.buildColumnsMap(
                        select, additionalTableReferences,
                        elemSyntax.row.in
                    )
                )
            ];
        }
    }

    private tryParseFunctionCall(
        select: Select,
        additionalTableReferences: TableReference[],
        funcCallSyntax: any
    ) {
        if ( !(funcCallSyntax instanceof FunctionCall) ) {
            return;
        }

        const funcNameSyntax = funcCallSyntax.row.call;
        const funcName = funcNameSyntax.toString();

        let args = funcCallSyntax.row.arguments.map(argSql =>
            this.parseFunctionCallArgument(
                select,
                additionalTableReferences,
                funcName,
                argSql
            )
        );
        
        let where: Expression | undefined;
        if ( funcCallSyntax.row.filter ) {
            where = this.parse(
                select,
                additionalTableReferences,
                funcCallSyntax.row.filter
            );
        }

        let orderByItems: OrderByItem[] = [];
        if ( funcCallSyntax.row.orderBy ) {
            funcCallSyntax.row.orderBy.forEach(itemSyntax => {
                const nulls = itemSyntax.row.nulls as ("first" | "last" | undefined);
                const vector = itemSyntax.row.vector as ("asc" | "desc" | undefined);
                const expression = this.parse(
                    select,
                    additionalTableReferences,
                    itemSyntax.row.expression
                );

                const item = new OrderByItem({
                    type: vector,
                    expression,
                    nulls
                });
                orderByItems.push(item);
            })
        }

        let distinct = funcCallSyntax.row.form == "distinct";

        if ( funcsWithoutOrderBy.includes(funcName) ) {
            orderByItems = [];
        }

        if ( funcName === "sum" ) {
            orderByItems = [];
        }

        // min(distinct x) = min(x)
        // max(distinct x) = max(x)
        // bool_or(distinct x) = bool_or(x)
        // bool_and(distinct x) = bool_and(x)
        const funcsWithoutDistinct = [
            "max",
            "min",
            "bool_or",
            "bool_and"
        ];
        if ( funcsWithoutDistinct.includes(funcName) ) {
            distinct = false;
        }

        // count(id_client) = count(id_partner) = count(*)
        // count(distinct id_client) != count(id_partner)
        if ( funcName === "count" && !distinct ) {
            args = [Expression.unknown("*")];
        }


        const funcCall = new FuncCall(
            funcName,
            args,
            where,
            distinct,
            orderByItems.length ? 
                new OrderBy(orderByItems) : 
                undefined
        );
        return funcCall;
    }

    private tryParsePgArray(
        select: Select,
        additionalTableReferences: TableReference[],
        elemSyntax: any
    ) {
        if ( !(elemSyntax instanceof PgArray) ) {
            return;
        }

        const content = elemSyntax.row.array.map(expressionSyntax =>
            this.parse(
                select,
                additionalTableReferences, 
                expressionSyntax.toString()
            )
        );
        return new ArrayElement(content);
    }

    private tryParseExtract(
        select: Select,
        additionalTableReferences: TableReference[],
        elemSyntax: any
    ) {
        if ( !(elemSyntax instanceof ExtractSyntax) ) {
            return;
        }

        const extract = elemSyntax.row.extract;
        const from = this.parse(
            select, additionalTableReferences,
            elemSyntax.row.from
        );
        return new Extract(extract, from);
    }

    private tryParseCaseWhen(
        select: Select,
        additionalTableReferences: TableReference[],
        elemSyntax: any
    ) {
        if ( !(elemSyntax instanceof CaseWhenSyntax) ) {
            return;
        }

        return new CaseWhen({
            cases: elemSyntax.row.case.map(caseSyntax => ({
                when: this.parse(
                    select,
                    additionalTableReferences, 
                    caseSyntax.row.when
                ),
                then: this.parse(
                    select,
                    additionalTableReferences, 
                    caseSyntax.row.then
                )
            })),
            else: elemSyntax.row.else ? this.parse(
                select,
                additionalTableReferences, 
                elemSyntax.row.else
            ) : undefined
        });
    }

    private tryParseColumnRef(
        select: Select,
        additionalTableReferences: TableReference[],
        elemSyntax: any
    ) {
        if ( !(elemSyntax instanceof ColumnLink) ) {
            return;
        }

        strict.ok( !elemSyntax.row.allColumns );
        return this.columnReferenceParser.parse(
            select, additionalTableReferences,
            elemSyntax
        );
    }

    private parseUnknown(
        select: Select,
        additionalTableReferences: TableReference[],
        elemSyntax: any
    ) {
        return this.unknownExpressionElementParser.parse(
            select,
            additionalTableReferences,
            elemSyntax 
        );
    }

    private parseFunctionCallArgument(
        select: Select,
        additionalTableReferences: TableReference[],
        funcName: string,
        argSql: Operand
    ) {
        if ( funcName === "count" && (argSql.toString()).trim() === "*" ) {
            return new Expression([
                new UnknownExpressionElement(
                    new ColumnLink({
                        row: {column: []}
                    })
                )
            ]);
        }

        return this.parse(
            select,
            additionalTableReferences,
            argSql
        );
    }
}

function toOperand(input: string | ExpressionSyntax | Operand) {
    if ( typeof input === "string" ) {
        return Sql.code(input).parse(ExpressionSyntax).operand();
    }
    if ( input instanceof ExpressionSyntax ) {
        return input.operand();
    }
    return input;
}