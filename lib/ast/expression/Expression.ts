import { Operator } from "./Operator";
import { FuncCall } from "./FuncCall";
import { IExpressionElement } from "./interface";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { Spaces } from "../Spaces";
import { UnknownExpressionElement } from "./UnknownExpressionElement";

type ConditionElementType = string | IExpressionElement;

export class Expression extends AbstractExpressionElement {

    static and(conditions: ConditionElementType[]) {
        return Expression.condition("and", conditions);
    }

    static or(conditions: ConditionElementType[]) {
        return Expression.condition("or", conditions);
    }

    static unknown(sql: string) {
        const unknownElement = UnknownExpressionElement.fromSql(sql);
        return new Expression([unknownElement]);
    }

    static funcCall(name: string, args: Expression[]) {
        const funcElem = new FuncCall(
            name,
            args
        );
        return new Expression([funcElem]);
    }

    private static condition(operator: string, conditions: ConditionElementType[]) {

        const elements: IExpressionElement[] = [];
        for (let i = 0, n = conditions.length; i < n; i++) {

            if ( i > 0 ) {
                elements.push( new Operator(operator) );
            }

            const sql = conditions[ i ];
            if ( typeof sql === "string" ) {
                elements.push(
                    UnknownExpressionElement.fromSql( sql )
                );
            }
            else {
                elements.push( sql );
            }
        }

        return new Expression(elements);
    }

    private elements: IExpressionElement[];
    constructor(elements: IExpressionElement[] = []) {
        super();
        this.elements = elements;
    }

    protected children() {
        return this.elements;
    }

    and(otherExpression?: Expression): Expression {
        if ( !otherExpression ) {
            return this;
        }

        const canCombineToPlainCondition = (
            this.onlyOperators("and") &&
            otherExpression.onlyOperators("and")
        );
        if ( canCombineToPlainCondition ) {
            return new Expression([
                ...this.elements,
                new Operator("and"),
                ...otherExpression.elements
            ]);
        }

        return new Expression([
            this,
            new Operator("and"),
            otherExpression
        ]);
    }

    isFuncCall() {
        return (
            this.elements.length === 1 &&
            this.elements[0] instanceof FuncCall
        );
    }

    isBinary(operator: string) {
        const isBinaryExpression = (
            this.elements.length === 3 &&
            this.elements[1] instanceof Operator &&
            this.elements[1].toString() === operator
        );
        return isBinaryExpression;
    }

    getOperands() {
        return this.elements.filter(elem =>
            !(elem instanceof Operator)
        );
    }

    isEmpty(): boolean {
        if ( this.elements.length === 0 ) {
            return true;
        }

        if ( this.elements.length === 1 ) {
            const firstElem = this.elements[0];
            if ( firstElem instanceof Expression ) {
                return firstElem.isEmpty();
            }
        }

        return false;
    }

    replaceFuncCall(replaceFunc: FuncCall, toSql: string): Expression {
        const newElements = this.elements.map(elem => 
            elem.replaceFuncCall(replaceFunc, toSql)
        );
        return new Expression(newElements);
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ): Expression {
        const newElements = this.elements.map(elem => 
            elem.replaceTable(replaceTable, toTable)
        );
        return new Expression(newElements);
    }

    replaceColumn(replaceColumn: string, toSql: string): Expression {
        const newElements = this.elements.map(elem => 
            elem.replaceColumn(replaceColumn, toSql)
        );
        return new Expression(newElements);
    }

    splitBy(operator: string) {
        const conditions: Expression[] = [];
        
        let currentCondition: any[] = [];
        for (const element of this.elements) {

            if ( element instanceof Operator ) {
                if ( element.toString() === operator ) {

                    if ( currentCondition.length ) {
                        const condition = new Expression( currentCondition );
                        conditions.push(condition);
                    }
                    
                    currentCondition = [];
                    continue;
                }
            }

            currentCondition.push(element);
        }

        if ( currentCondition.length ) {
            const condition = new Expression( currentCondition );
            conditions.push(condition);
        }

        return conditions;
    }

    clone() {
        return new Expression(
            this.elements.map(elem => elem.clone())
        );
    }

    template(spaces: Spaces) {
        const lines: string[] = [];

        let line = "";
        for (const elem of this.elements) {

            const isConditionOperator = (
                ["and", "or"].includes(elem.toString())
            );
            if ( isConditionOperator ) {
                lines.push(spaces + line.trim());
                lines.push(spaces + elem.toString());
                line = "";
                continue;
            }

            if ( elem instanceof Expression ) {
                // TODO: maybe exists another operators?
                if ( elem.hasOperator("-") ) {
                    line += ` (${ elem })`;
                    continue;
                }
                if ( /^[\w\.]+$/i.test(elem.toString()) ) {
                    line += ` ${ elem }`;
                    continue;
                }

                if ( line.trim() ) {
                    lines.push( spaces + line.trim() );
                }

                const subExpression = elem;

                if ( subExpression.onlyOperators("or") ) {
                    
                    lines.push(spaces + "(");
                    lines.push(
                        ...subExpression.template(
                            spaces.plusOneLevel()
                        )
                    );
                    lines.push(spaces + ")");

                }
                else {
                    lines.push(
                        ...subExpression.template( spaces )
                    );
                }

                line = "";
                continue;
            }
            else {
                line += " " + elem.toSQL(spaces);
            }
        }

        if ( line.trim() ) {
            lines.push( spaces + line.trim() );
        }

        return lines.filter(someLine => !!someLine.trim());
    }

    private onlyOperators(onlyOperator: string) {
        const operators = this.elements.filter(elem => 
            elem instanceof Operator
        );
        return (
            operators.length && 
            operators.every(operator =>
                operator.toString() === onlyOperator
            )
        );
    }

    private hasOperator(operator: string) {
        return this.elements.some(elem => 
            elem instanceof Operator &&
            elem.toString() === operator
        );
    }
}

