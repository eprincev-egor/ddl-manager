import AbstractTableModel from "../../objects/AbstractTableModel";
import {
    CreateTable,
    Extension,
    PrimaryKeyConstraint,
    UniqueConstraint,
    CheckConstraint,
    ForeignKeyConstraint
} from "grapeql-lang";

export function prepareAbstractTable(
    filePath: string,
    tableName: string,
    parsedTable: CreateTable | Extension
): AbstractTableModel<any>["TInputData"] {
        
    // get primary key constraint from parsedTable.constraints
    // or from columns
    let primaryKeyConstraint = parsedTable.row.constraints.find(constraint => 
        constraint instanceof PrimaryKeyConstraint
    ) as PrimaryKeyConstraint;
    if ( !primaryKeyConstraint ) {
        parsedTable.row.columns.forEach(column => {
            if ( column.get("primaryKey") ) {
                primaryKeyConstraint = column.get("primaryKey");
            }
        });
    }
    
    let primaryKey = null;
    if ( primaryKeyConstraint ) {
        primaryKey = primaryKeyConstraint.get("primaryKey").map(column => 
            column.toString()
        );
    }

    // table test (...) deprecated (...)
    const deprecatedColumns = parsedTable.row.deprecatedColumns.map(columnName =>
        columnName.toString()
    );

    // table test (...) values (...)
    let rows = null;
    if ( parsedTable instanceof CreateTable ) {
        const parsedRows = parsedTable.row.values;
        if ( parsedRows && parsedRows.length ) {
            rows = parsedRows;
        }
    }

    // table (..., constraint x unique(...))
    const parsedUniqueConstraints = parsedTable.row.constraints.filter(constraint =>
        constraint instanceof UniqueConstraint
    ) as UniqueConstraint[];

    parsedTable.row.columns.forEach(column => {
        const uniqueConstraint = column.get("unique");
        
        if ( uniqueConstraint ) {
            parsedUniqueConstraints.push(uniqueConstraint);
        }
    });

    const uniqueConstraints = parsedUniqueConstraints.map(uniqueConstraint => {
        let name = (
            uniqueConstraint.get("name") &&
            uniqueConstraint.get("name").toString()
        );
        if ( !name ) {
            name = (
                tableName.toString() + 
                "_" +
                uniqueConstraint.get("column").toString() + 
                "_key"
            );
        }

        return {
            filePath,
            identify: name,
            name,
            parsed: uniqueConstraint,
            unique: uniqueConstraint.get("unique").map(column =>
                column.toString()
            )
        };
    });

    const parsedCheckConstraints = parsedTable.row.constraints.filter(constraint =>
        constraint instanceof CheckConstraint
    ) as CheckConstraint[];

    parsedTable.row.columns.forEach(column => {
        const checkConstraint = column.get("check");

        if ( checkConstraint ) {
            parsedCheckConstraints.push(checkConstraint);
        }
    });

    const checkConstraints = parsedCheckConstraints.map(checkConstraint => {
        let name = (
            checkConstraint.get("name") &&
            checkConstraint.get("name").toString()
        );

        if ( !name ) {
            name = (
                tableName.toString() + 
                "_" +
                checkConstraint.get("column").toString() + 
                "_check"
            );
        }

        return {
            filePath,
            identify: name,
            name,
            parsed: checkConstraint
        };
    });

    const parsedForeignKeys = parsedTable.row.constraints.filter(constraint => 
        constraint instanceof ForeignKeyConstraint
    ) as ForeignKeyConstraint[];

    parsedTable.row.columns.forEach(column => {
        const foreignKey = column.get("foreignKey");

        if ( foreignKey ) {
            parsedForeignKeys.push( foreignKey );
        }
    });

    const foreignKeys = parsedForeignKeys.map(foreignKey => {
        let name = (
            foreignKey.get("name") &&
            foreignKey.get("name").toString()
        );
        
        if ( !name ) {
            name = (
                tableName.toString() + 
                "_" +
                foreignKey.get("column").toString() + 
                "_fkey"
            );
        }

        const selfColumns = foreignKey.get("columns").map(column =>
            column.toString()
        );

        let referenceColumns = [];
        if ( foreignKey.row.referenceColumns ) {
            referenceColumns = foreignKey.get("referenceColumns").map(refColumn => 
                refColumn.toString()
            );
        }
        if ( !referenceColumns.length ) {
            referenceColumns = ["id"];
        }

        return {
            filePath,
            identify: name,
            name,
            columns: selfColumns,
            referenceTableIdentify: foreignKey.row.referenceTable.toString(),
            referenceColumns,
            parsed: foreignKey
        };
    });


    return {
        columns: parsedTable.row.columns.map(parseColumn => {
            const key = parseColumn.get("name").toString();

            return {
                filePath,
                identify: key,
                key,
                type: parseColumn.get("type").toString(),
                parsed: parseColumn,
                nulls: parseColumn.get("nulls")
            };
        }),
        primaryKey,
        deprecated: parsedTable.row.deprecated,
        deprecatedColumns,
        rows,
        uniqueConstraints,
        checkConstraints,
        foreignKeysConstraints: foreignKeys
    };
}