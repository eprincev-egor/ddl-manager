import { Operator } from "./Operator";
import { FuncCall } from "./FuncCall";
import { IExpressionElement } from "./interface";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { Spaces } from "../Spaces";
import { IColumnsMap, UnknownExpressionElement } from "./UnknownExpressionElement";
import { ColumnReference } from "./ColumnReference";
import { Exists } from "./Exists";

export type ConditionElementType = string | IExpressionElement;

export class Expression extends AbstractExpressionElement {

    static and(conditions: ConditionElementType[]) {
        return Expression.condition("and", conditions);
    }

    static or(conditions: ConditionElementType[]) {
        return Expression.condition("or", conditions);
    }

    static unknown(sql: string, columnsMap: IColumnsMap = {}) {
        const unknownElement = UnknownExpressionElement.fromSql(sql, columnsMap);
        return new Expression([unknownElement]);
    }

    static funcCall(name: string, args: Expression[]) {
        const funcElem = new FuncCall(
            name,
            args
        );
        return new Expression([funcElem]);
    }

    private static condition(operator: string, conditions: ConditionElementType[]): Expression {

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

        return new Expression(elements).extrude();
    }

    readonly elements: IExpressionElement[];
    constructor(elements: IExpressionElement[] = []) {
        super();
        this.elements = elements;
    }

    children() {
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

    getExplicitCastType(): string | undefined {
        const twoLastElements = this.elements.slice(-2);
        const isCasting = (
            twoLastElements.length === 2 && 
            twoLastElements[0].toString() === "::"
        );
        if ( isCasting ) {
            return twoLastElements[1].toString();
        }
    }

    isCoalesce() {
        if ( !this.isFuncCall() ) {
            return false;
        }
        const funcCall = this.getFuncCalls()[0] as FuncCall;
        return funcCall.name === "coalesce";
    }

    isFuncCall(): boolean {
        const elements = this.getElementsWithoutCasts();
        const firstElem = elements[0];
        return (
            elements.length === 1 &&
            (
                firstElem instanceof FuncCall
                ||
                firstElem instanceof Expression &&
                firstElem.isFuncCall()
            )
        );
    }

    isColumnReference() {
        return (
            this.elements.length === 1 &&
            this.elements[0] instanceof ColumnReference
        );
    }

    isArrayItemOfColumnReference() {
        return (
            this.elements.length === 2 &&
            this.elements[0] instanceof ColumnReference &&
            /^\[\s*\d+\s*\]$/.test(this.elements[1].toString().trim())
        );
    }

    isIn() {
        return (
            this.elements.length === 2 &&
            /^\s*in\s*\([^\)]+\)\s*$/.test(this.elements[1].toString())
        );
    }

    isNotExists() {
        return (
            this.elements.length === 2 &&
            this.elements[0].toString() === "not" &&
            this.elements[1] instanceof Exists
        );
    }

    isBinary(operator: string) {
        const elems = (
            operator === "::" ? 
                this.elements :
                this.getElementsWithoutCasts()
        );

        const isBinaryExpression = (
            elems.length === 3 &&
            elems[1] instanceof Operator &&
            elems[1].toString() === operator
        );
        return isBinaryExpression;
    }

    isEqualAny() {
        const elems = this.getElementsWithoutCasts();

        const isEqualAnyExpression = (
            elems.length === 3 &&
            elems[1] instanceof Operator &&
            elems[1].toString() === "=" &&
            /^\s*any\s*\(/i.test(elems[2].toString())
        );
        return isEqualAnyExpression;
    }

    getOperands() {
        const operands: IExpressionElement[] = [];

        for (let i = 0, n = this.elements.length; i < n; i++) {
            const elem = this.elements[i];
            const nextElem = this.elements[i + 1];

            if ( nextElem instanceof Operator && nextElem.toString() === "::" ) {
                const castOperator = nextElem;
                const castType = this.elements[i + 2];

                const elemWithCasting = new Expression([
                    elem,
                    castOperator,
                    castType
                ]);
                operands.push(elemWithCasting);

                i += 2;
                continue;
            }

            if ( elem instanceof Operator ) {
                continue;
            }

            operands.push(elem);
        }

        return operands;
    }

    private getElementsWithoutCasts() {
        const expressionElementsWithoutCasts = this.elements.slice();

        for (let i = 0, n = expressionElementsWithoutCasts.length; i < n; i++) {
            const elem = expressionElementsWithoutCasts[i];

            if ( elem instanceof Operator && elem.toString() === "::" ) {
                expressionElementsWithoutCasts.splice(i, 2);
                n -= 2;
                i--;
            }
        }

        return expressionElementsWithoutCasts;
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

    replaceColumn(replaceColumn: ColumnReference, toSql: IExpressionElement): Expression {
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

    extrude(): Expression {
        const singleElement = this.elements[0];
        if ( this.elements.length === 1 && singleElement instanceof Expression )  {
            return singleElement.extrude();
        }

        return this;
    }

    clone(newElements?: IExpressionElement[]) {
        return new Expression(
            newElements || this.elements.map(elem => elem.clone())
        );
    }

    needWrapToBrackets() {
        return (
            /^case\s/.test( this.toString().trim() )
        );
    }

    template(spaces: Spaces) {
        const lines: string[] = [];

        let line = "";
        for (const elem of this.elements) {

            const isConditionOperator = (
                elem instanceof Operator &&
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

                if ( subExpression.hasOperator("or") ) {
                    
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

