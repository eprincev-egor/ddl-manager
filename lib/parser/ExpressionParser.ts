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

    parse(input: string | ExpressionSyntax | Operand): Expression {
        const sql = toOperand(input);

        const elements = this.parseElements(sql);
        if ( elements.length === 1 && elements[0] instanceof Expression ) {
            return elements[0] as Expression;
        }

        return new Expression(elements);
    }

    private parseElements(elemSyntax: any): IExpressionElement[] {
        if ( elemSyntax instanceof SubExpression ) {
            return [new Expression(
                this.parseElements(
                    elemSyntax.row.subExpression
                ), true
            )];
        }

        return this.tryParseBinaryOperator( elemSyntax) || [
            this.tryParseFunctionCall(elemSyntax) ||
            this.tryParsePgArray(elemSyntax) ||
            this.tryParseExtract(elemSyntax) ||
            this.tryParseCaseWhen(elemSyntax) ||
            this.tryParseColumnRef(elemSyntax) ||
            this.parseUnknown(elemSyntax)
        ];
    }

    private tryParseBinaryOperator(elemSyntax: any) {
        if ( elemSyntax instanceof BinaryOperator ) {
            return [
                ...this.parseElements(
                    elemSyntax.row.left
                ),
                new Operator(
                    elemSyntax.row.operator
                ),
                ...this.parseElements(
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
                    elemSyntax.row.operand
                )
            ];
        }

        if ( elemSyntax instanceof PostUnaryOperator ) {
            return [
                ...this.parseElements(
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
                    elemSyntax.row.cast
                ),
                new Operator("::"),
                UnknownExpressionElement.fromSql(elemSyntax.row.to.toString())
            ];
        }

        if ( elemSyntax instanceof SquareBrackets ) {
            return [
                ...this.parseElements(
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
                    elemSyntax.row.operand
                ),
                new Operator("="),
                UnknownExpressionElement.fromSql(
                    "any(" + elemSyntax.row.equalAny.toString() + ")",
                    this.unknownExpressionElementParser.buildColumnsMap(
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
                    elemSyntax.row.operand
                ),
                UnknownExpressionElement.fromSql(
                    unknownSql,
                    this.unknownExpressionElementParser.buildColumnsMap(
                        elemSyntax.row.in
                    )
                )
            ];
        }
    }

    private tryParseFunctionCall(funcCallSyntax: any) {
        if ( !(funcCallSyntax instanceof FunctionCall) ) {
            return;
        }

        const funcNameSyntax = funcCallSyntax.row.call;
        const funcName = funcNameSyntax.toString();

        let args = funcCallSyntax.row.arguments.map(argSql =>
            this.parseFunctionCallArgument(
                funcName,
                argSql
            )
        );
        
        let where: Expression | undefined;
        if ( funcCallSyntax.row.filter ) {
            where = this.parse(
                funcCallSyntax.row.filter
            );
        }

        let orderByItems: OrderByItem[] = [];
        if ( funcCallSyntax.row.orderBy ) {
            funcCallSyntax.row.orderBy.forEach(itemSyntax => {
                const nulls = itemSyntax.row.nulls as ("first" | "last" | undefined);
                const vector = itemSyntax.row.vector as ("asc" | "desc" | undefined);
                const expression = this.parse(
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

    private tryParsePgArray(elemSyntax: any) {
        if ( !(elemSyntax instanceof PgArray) ) {
            return;
        }

        const content = elemSyntax.row.array.map(expression => 
            this.parse(expression)
        );
        return new ArrayElement(content);
    }

    private tryParseExtract(elemSyntax: any) {
        if ( !(elemSyntax instanceof ExtractSyntax) ) {
            return;
        }

        const extract = elemSyntax.row.extract;
        const from = this.parse(
            elemSyntax.row.from
        );
        return new Extract(extract, from);
    }

    private tryParseCaseWhen(elemSyntax: any) {
        if ( !(elemSyntax instanceof CaseWhenSyntax) ) {
            return;
        }

        return new CaseWhen({
            cases: elemSyntax.row.case.map(caseSyntax => ({
                when: this.parse(
                    caseSyntax.row.when
                ),
                then: this.parse(
                    caseSyntax.row.then
                )
            })),
            else: elemSyntax.row.else ? this.parse(
                elemSyntax.row.else
            ) : undefined
        });
    }

    private tryParseColumnRef(elemSyntax: any) {
        if ( !(elemSyntax instanceof ColumnLink) ) {
            return;
        }

        strict.ok( !elemSyntax.row.allColumns );
        return this.columnReferenceParser.parse(elemSyntax);
    }

    private parseUnknown(elemSyntax: any) {
        return this.unknownExpressionElementParser.parse(elemSyntax);
    }

    private parseFunctionCallArgument(
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

        return this.parse(argSql);
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