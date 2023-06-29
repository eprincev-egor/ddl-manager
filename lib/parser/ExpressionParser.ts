import {
    GrapeQLCoach,
    Expression as ExpressionSyntax,
    FunctionCall,
    ColumnLink,
    Operator as OperatorSyntax,
    CaseWhen as CaseWhenSyntax,
    FunctionLink,
    PgArray,
    Extract as ExtractSyntax
} from "grapeql-lang";
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
import assert from "assert";
import { ArrayElement } from "../ast/expression/ArrayElement";

export class ExpressionParser {

    private columnReferenceParser = new ColumnReferenceParser();

    parse(
        select: Select,
        additionalTableReferences: TableReference[],
        sql: string | ExpressionSyntax
    ) {
        const elements: IExpressionElement[] = [];

        const unknownExpressionElementParser = new UnknownExpressionElementParser();

        if ( typeof sql === "string" ) {
            sql = new GrapeQLCoach(sql)
                .parse(ExpressionSyntax);
        }

        const syntaxElements = sql.get("elements") as any[];
        
        for (const elemSyntax of syntaxElements) {
            let elem!: IExpressionElement;

            if ( elemSyntax instanceof FunctionCall ) {
                elem = this.parseFunctionCall(
                    select, 
                    additionalTableReferences, 
                    elemSyntax
                );
            }
            else if ( elemSyntax instanceof PgArray ) {
                const content = (elemSyntax.get("array") || [])
                    .map(expressionSyntax =>
                        this.parse(
                            select,
                            additionalTableReferences, 
                            expressionSyntax.toString()
                        )
                    );
                elem = new ArrayElement(content);
            }
            else if ( elemSyntax instanceof ExtractSyntax ) {
                const extract = elemSyntax.get("extract")!;
                const from = this.parse(
                    select, additionalTableReferences,
                    elemSyntax.get("from")!
                );
                elem = new Extract(extract, from);
            }
            else if ( elemSyntax instanceof CaseWhenSyntax ) {
                elem = new CaseWhen({
                    cases: elemSyntax.get("case")!.map(caseSyntax => ({
                        when: this.parse(
                            select,
                            additionalTableReferences, 
                            caseSyntax.get("when")!.toString()
                        ),
                        then: this.parse(
                            select,
                            additionalTableReferences, 
                            caseSyntax.get("then")!.toString()
                        )
                    })),
                    else: elemSyntax.get("else") ? this.parse(
                        select,
                        additionalTableReferences, 
                        elemSyntax.get("else")!.toString()
                    ) : undefined
                })
            }
            else if ( elemSyntax instanceof ExpressionSyntax ) {
                elem = this.parse(
                    select,
                    additionalTableReferences, 
                    elemSyntax.toString()
                );
            }
            else if ( elemSyntax instanceof ColumnLink ) {
                assert.ok( !elemSyntax.isStar() );
                elem = this.columnReferenceParser.parse(select, additionalTableReferences, elemSyntax);
            }
            else if ( elemSyntax instanceof OperatorSyntax ) {
                elem = new Operator( elemSyntax.toString() as string );
            }
            else {
                elem = unknownExpressionElementParser.parse(
                    select,
                    additionalTableReferences,
                    elemSyntax 
                );
            }

            elements.push(elem);
        }

        const expression = new Expression(
            elements,
            !!sql.row.brackets
        );
        return expression;
    }

    private parseFunctionCall(
        select: Select,
        additionalTableReferences: TableReference[],
        funcCallSyntax: FunctionCall
    ): FuncCall {
        const funcNameSyntax = funcCallSyntax.get("function") as FunctionLink;
        const funcName = funcNameSyntax.toString();
        
        const argsSyntaxes = (funcCallSyntax.get("arguments") || []);
        let args = argsSyntaxes.map(argSql =>
            this.parseFunctionCallArgument(
                select,
                additionalTableReferences,
                funcName,
                argSql
            )
        );
        
        let where: Expression | undefined;
        if ( funcCallSyntax.row.where ) {
            where = this.parse(
                select,
                additionalTableReferences,
                funcCallSyntax.row.where
            );
        }

        let orderByItems: OrderByItem[] = [];
        if ( funcCallSyntax.row.orderBy ) {
            funcCallSyntax.row.orderBy.forEach(itemSyntax => {
                const nulls = itemSyntax.row.nulls as ("first" | "last" | undefined);
                const vector = itemSyntax.row.vector as ("asc" | "desc" | undefined);
                const expressionSyntax = itemSyntax.row.expression as ExpressionSyntax;
                const expression = this.parse(
                    select,
                    additionalTableReferences,
                    expressionSyntax
                );

                const item = new OrderByItem({
                    type: vector,
                    expression,
                    nulls
                });
                orderByItems.push(item);
            })
        }

        let distinct = !!funcCallSyntax.row.distinct;

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

    private parseFunctionCallArgument(
        select: Select,
        additionalTableReferences: TableReference[],
        funcName: string,
        argSql: ExpressionSyntax
    ) {
        if ( funcName === "count" && (argSql.toString()).trim() === "*" ) {
            return new Expression([
                new UnknownExpressionElement(
                    new ColumnLink({
                        star: true,
                        link: []
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