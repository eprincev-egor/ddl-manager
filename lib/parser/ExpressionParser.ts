import {
    GrapeQLCoach,
    Expression as ExpressionSyntax,
    FunctionCall,
    ColumnLink,
    Operator as OperatorSyntax,
    FunctionLink
} from "grapeql-lang";
import { 
    IExpressionElement,
    FuncCall,
    Select,
    Operator,
    Expression,
    UnknownExpressionElement,
    IOrderByItem
} from "../ast";
import { UnknownExpressionElementParser } from "./UnknownExpressionElementParser";
import { ColumnReferenceParser } from "./ColumnReferenceParser";
import { TableReference } from "../database/schema/TableReference";
import assert from "assert";

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

        const expression = new Expression(elements);
        return expression;
    }

    private parseFunctionCall(
        select: Select,
        additionalTableReferences: TableReference[],
        funcCallSyntax: FunctionCall
    ): FuncCall {
        const funcNameSyntax = funcCallSyntax.get("function") as FunctionLink;
        const funcName = funcNameSyntax.toString();
        
        const args = (funcCallSyntax.get("arguments") || []);
        
        let where: Expression | undefined;
        if ( funcCallSyntax.row.where ) {
            where = this.parse(
                select,
                additionalTableReferences,
                funcCallSyntax.row.where
            );
        }

        const orderBy: Partial<IOrderByItem>[] = [];
        if ( funcCallSyntax.row.orderBy ) {
            funcCallSyntax.row.orderBy.forEach(itemSyntax => {
                const nulls = itemSyntax.row.nulls as ("first" | "last" | undefined);
                const usingSyntax = itemSyntax.row.using;
                const vector = itemSyntax.row.vector as ("asc" | "desc" | undefined);
                const expressionSyntax = itemSyntax.row.expression as ExpressionSyntax;
                const expression = this.parse(
                    select,
                    additionalTableReferences,
                    expressionSyntax
                );

                const item: Partial<IOrderByItem> = {
                    vector,
                    expression,
                    using: usingSyntax ? usingSyntax.toString() : undefined,
                    nulls
                };
                orderBy.push(item);
            })
        }

        const funcCall = new FuncCall(
            funcName,
            args.map(argSql =>
                this.parseFunctionCallArgument(
                    select,
                    additionalTableReferences,
                    funcName,
                    argSql
                )
            ),
            where,
            funcCallSyntax.row.distinct ? true : false,
            orderBy
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