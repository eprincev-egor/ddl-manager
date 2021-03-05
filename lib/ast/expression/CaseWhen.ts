import { Spaces } from "../Spaces";
import { Expression } from "./Expression";
import { AbstractExpressionElement } from "./AbstractExpressionElement";
import { flatMap } from "lodash";
import { IExpressionElement } from "./interface";
import { TableReference } from "../../database/schema/TableReference";
import { TableID } from "../../database/schema/TableID";
import { ColumnReference } from "./ColumnReference";
import { FuncCall } from "./FuncCall";

interface ICase {
    when: Expression;
    then: Expression;
}

export class CaseWhen extends AbstractExpressionElement {
    readonly cases: ICase[];
    readonly else?: Expression;

    constructor(params: {cases: ICase[], else?: Expression}) {
        super();
        this.cases = params.cases;
        this.else = params.else;
    }

    children() {
        const children: IExpressionElement[] = [];
        for (const caseElem of this.cases) {
            children.push( ...caseElem.when.children() );
            children.push( ...caseElem.then.children() );
        }

        if ( this.else ) {
            children.push(...this.else.children());
        }

        return children;
    }

    clone(changes: {
        cases?: ICase[];
        else?: Expression;
    } = {}) {
        return new CaseWhen({
            cases: changes.cases || this.cloneCases(),
            else: (
                "else" in changes ? 
                    changes.else :
                    this.cloneElse()
            )
        });
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        const changes:  {
            cases?: ICase[];
            else?: Expression;
        } = {};

        changes.cases = this.cases.map(caseElem => {
            const newWhen = caseElem.when.replaceTable(
                replaceTable, toTable
            );
            const newThen = caseElem.then.replaceTable(
                replaceTable, toTable
            );

            return {when: newWhen, then: newThen};
        });
        if ( this.else ) {
            changes.else = this.else.replaceTable(replaceTable, toTable);
        }
        return this.clone(changes);
    }

    replaceColumn(replaceColumn: ColumnReference, toSql: IExpressionElement) {
        const changes:  {
            cases?: ICase[];
            else?: Expression;
        } = {};

        changes.cases = this.cases.map(caseElem => {
            const newWhen = caseElem.when.replaceColumn(
                replaceColumn, toSql
            );
            const newThen = caseElem.then.replaceColumn(
                replaceColumn, toSql
            );

            return {when: newWhen, then: newThen};
        });
        if ( this.else ) {
            changes.else = this.else.replaceColumn(replaceColumn, toSql);
        }
        return this.clone(changes);
    }

    replaceFuncCall(replaceFunc: FuncCall, toSql: string) {
        const changes:  {
            cases?: ICase[];
            else?: Expression;
        } = {};

        changes.cases = this.cases.map(caseElem => {
            const newWhen = caseElem.when.replaceFuncCall(
                replaceFunc, toSql
            );
            const newThen = caseElem.then.replaceFuncCall(
                replaceFunc, toSql
            );

            return {when: newWhen, then: newThen};
        });
        if ( this.else ) {
            changes.else = this.else.replaceFuncCall(replaceFunc, toSql);
        }
        return this.clone(changes);
    }

    private cloneElse() {
        if ( this.else ) {
            return this.else.clone();
        }
    }

    private cloneCases() {
        return this.cases.map(someCase => ({
            when: someCase.when.clone(),
            then: someCase.then.clone()
        }));
    }

    template(spaces: Spaces): string[] {
        return [
            spaces + "case",
            ...flatMap(this.cases, someCase =>
                this.printCase(someCase, spaces)
            ),
            ...this.printElse(spaces),
            spaces + "end"
        ];
    }

    private printCase(someCase: ICase, spaces: Spaces) {
        return [
            spaces.plusOneLevel() + "when",
            someCase.when.toSQL(
                spaces
                    .plusOneLevel()
                    .plusOneLevel()
            ),
            spaces.plusOneLevel() + "then",
            someCase.then.toSQL(
                spaces
                    .plusOneLevel()
                    .plusOneLevel()
            )
        ];
    }

    private printElse(spaces: Spaces) {
        if ( !this.else ) {
            return [];
        }

        return [
            spaces.plusOneLevel() + "else",
            ...this.else.template(
                spaces
                    .plusOneLevel()
                    .plusOneLevel()
            )
        ];
    }
}