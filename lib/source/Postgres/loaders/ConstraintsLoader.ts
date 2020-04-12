
import fs from "fs";
import { ILoader } from "./ILoader";
import { IDBDriver } from "../../../common";
import { CheckConstraintDBO } from "../objects/CheckConstraintDBO";
import { PrimaryKeyDBO } from "../objects/PrimaryKeyDBO";
import { UniqueConstraintDBO } from "../objects/UniqueConstraintDBO";
import { ForeignKeyDBO } from "../objects/ForeignKeyDBO";
import { GrapeQLCoach, Expression } from "grapeql-lang";

const sqlConstraintsPath = __dirname + "/sql/select-constraints.sql";
const selectConstraintsSQL = fs.readFileSync(sqlConstraintsPath).toString();

type TConstraint = (
    PrimaryKeyDBO |
    UniqueConstraintDBO |
    CheckConstraintDBO |
    ForeignKeyDBO
);

interface IConstraintRow {
    constraint_type: "FOREIGN KEY" | "UNIQUE" | "PRIMARY KEY" | "CHECK";
    constraint_name: string;
    table_identify: string;
    table_schema: string;
    table_name: string;
    update_rule: string;
    delete_rule: string;
    columns: string[];
    reference_columns: string[];
    reference_table: string;
    check_clause: string;
}

export class ConstraintsLoader
implements ILoader {
    private driver: IDBDriver;

    constructor(driver: IDBDriver) {
        this.driver = driver;
    }
    
    async load(): Promise<TConstraint[]> {
        const rows = await this.driver.query<IConstraintRow>(selectConstraintsSQL);
        const objects = rows.map(row => 
            this.createConstraint(row)
        );
        return objects;
    }

    private createConstraint(row: IConstraintRow): TConstraint {
        let constraint;

        if ( row.constraint_type === "PRIMARY KEY" ) {
            constraint = this.createPrimaryKey(row);
        }

        if ( row.constraint_type === "UNIQUE" ) {
            constraint = this.createUniqueConstraint(row);
        }

        if ( row.constraint_type === "CHECK" ) {
            constraint = this.createCheckConstraint(row);
        }

        if ( row.constraint_type === "FOREIGN KEY" ) {
            constraint = this.createForeignKey(row);
        }

        return constraint;
    }

    private createPrimaryKey(row: IConstraintRow) {
        const primaryKey = new PrimaryKeyDBO({
            table: row.table_identify,
            name: row.constraint_name,
            primaryKey: row.columns
        });
        return primaryKey;
    }

    private createUniqueConstraint(row: IConstraintRow) {
        const uniqueConstraint = new UniqueConstraintDBO({
            table: row.table_identify,
            name: row.constraint_name,
            unique: row.columns
        });
        return uniqueConstraint;
    }

    private createCheckConstraint(row: IConstraintRow) {
        const checkString = extrudeBracketsFromCheckClause(row.check_clause);
        const checkConstraint = new CheckConstraintDBO({
            table: row.table_identify,
            name: row.constraint_name,
            check: checkString.trim()
        });
        return checkConstraint;
    }

    private createForeignKey(row: IConstraintRow) {
        const foreignKey = new ForeignKeyDBO({
            table: row.table_identify,
            name: row.constraint_name,
            columns: row.columns,
            referenceColumns: row.reference_columns,
            referenceTable: row.reference_table
        });
        return foreignKey;
    }
}


function extrudeBracketsFromCheckClause(checkClause: string): string {
    const coach = new GrapeQLCoach(checkClause);
    const expression = coach.parse(Expression);
    const checkString = expression.toString();
    return checkString;
}