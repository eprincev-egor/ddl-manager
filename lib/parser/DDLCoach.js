"use strict";

const Coach = require("./Coach");

class DDLCoach extends Coach {
    static parseSqlFile(sql) {
        let coach = new DDLCoach(sql);
        coach.replaceComments();
        let sqlFile = coach.parseSqlFile();

        return sqlFile.toJSON();
    }

    parseSchemaName() {
        let coach = this;
        
        // name
        let i = coach.i;
        let objectLink = coach.parseObjectLink();
        if ( 
            objectLink.link.length != 2 &&
            objectLink.link.length != 1
        ) {
            coach.i = i;
            coach.throwError("invalid name " + objectLink.toString());
        }

        let schema = "public";
        let name = objectLink.link[0].toLowerCase();
        if ( objectLink.link.length == 2 ) {
            schema = name;
            name = objectLink.link[1].toLowerCase();
        }
        
        return {schema, name};
    }

    replaceComments() {
        let coach = this;
        let startIndex = coach.i;
        let newStr = coach.str.split("");

        for (; coach.i < coach.n; coach.i++) {
            let i = coach.i;

            // ignore comments inside function
            if ( coach.isCreateFunction() ) {
                coach.parseCreateFunction();
            }

            if ( coach.isCreateTrigger() ) {
                coach.parseCreateTrigger();
            }

            if ( coach.isComment() ) {
                coach.parseComment();

                let length = coach.i - i;
                // safe \n\r
                let spaceStr = coach.str.slice(i, i + length).replace(/[^\n\r]/g, " ");

                newStr.splice.apply(newStr, [i, length].concat( spaceStr.split("") ));
            }
        }

        coach.i = startIndex;
        coach.str = newStr.join("");
    }
}

DDLCoach.addSyntax = Coach.addSyntax;

DDLCoach.addSyntax("DataType", require("./syntax/DataType"));
DDLCoach.addSyntax("PgNumber", require("./syntax/PgNumber"));
DDLCoach.addSyntax("PgArgument", require("./syntax/PgArgument"));
DDLCoach.addSyntax("PgString", require("./syntax/PgString"));
DDLCoach.addSyntax("PgReturns", require("./syntax/PgReturns"));
DDLCoach.addSyntax("CreateFunction", require("./syntax/CreateFunction"));
DDLCoach.addSyntax("DoubleQuotes", require("./syntax/DoubleQuotes"));
DDLCoach.addSyntax("ObjectName", require("./syntax/ObjectName"));
DDLCoach.addSyntax("ObjectLink", require("./syntax/ObjectLink"));
DDLCoach.addSyntax("Any", require("./syntax/Any"));
DDLCoach.addSyntax("Between", require("./syntax/Between"));
DDLCoach.addSyntax("Boolean", require("./syntax/Boolean"));
DDLCoach.addSyntax("CaseWhen", require("./syntax/CaseWhen"));
DDLCoach.addSyntax("CaseWhenElement", require("./syntax/CaseWhenElement"));
DDLCoach.addSyntax("Cast", require("./syntax/Cast"));
DDLCoach.addSyntax("ColumnLink", require("./syntax/ColumnLink"));
DDLCoach.addSyntax("Comment", require("./syntax/Comment"));
DDLCoach.addSyntax("Expression", require("./syntax/Expression"));
DDLCoach.addSyntax("Extract", require("./syntax/Extract"));
DDLCoach.addSyntax("FunctionCall", require("./syntax/FunctionCall"));
DDLCoach.addSyntax("In", require("./syntax/In"));
DDLCoach.addSyntax("Operator", require("./syntax/Operator"));
DDLCoach.addSyntax("PgArray", require("./syntax/PgArray"));
DDLCoach.addSyntax("PgNull", require("./syntax/PgNull"));
DDLCoach.addSyntax("SquareBrackets", require("./syntax/SquareBrackets"));
DDLCoach.addSyntax("Substring", require("./syntax/Substring"));
DDLCoach.addSyntax("ToType", require("./syntax/ToType"));
DDLCoach.addSyntax("FunctionLink", require("./syntax/FunctionLink"));
DDLCoach.addSyntax("CreateTrigger", require("./syntax/CreateTrigger"));
DDLCoach.addSyntax("SqlFile", require("./syntax/SqlFile"));

module.exports = DDLCoach;
