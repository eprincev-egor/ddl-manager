"use strict";


const testSyntax = require("../utils/testSyntax");

let tests = {};
// tests.DataType = require("./syntax/DataType");
// tests.PgNumber = require("./syntax/PgNumber");
// tests.PgArgument = require("./syntax/PgArgument");
// tests.PgString = require("./syntax/PgString");
// tests.PgReturns = require("./syntax/PgReturns");
// tests.CreateFunction = require("./syntax/CreateFunction");
// tests.DoubleQuotes = require("./syntax/DoubleQuotes");
// tests.ObjectName = require("./syntax/ObjectName");
// tests.ObjectLink = require("./syntax/ObjectLink");
// tests.Any = require("./syntax/Any");
// tests.Between = require("./syntax/Between");
// tests.Boolean = require("./syntax/Boolean");
// tests.CaseWhen = require("./syntax/CaseWhen");
// tests.CaseWhenElement = require("./syntax/CaseWhenElement");
// tests.Cast = require("./syntax/Cast");
// tests.Comment = require("./syntax/Comment");
// tests.Expression = require("./syntax/Expression");
// tests.Extract = require("./syntax/Extract");
// tests.FunctionCall = require("./syntax/FunctionCall");
// tests.In = require("./syntax/In");
// tests.Operator = require("./syntax/Operator");
// tests.PgArray = require("./syntax/PgArray");
// tests.PgNull = require("./syntax/PgNull");
// tests.SquareBrackets = require("./syntax/SquareBrackets");
// tests.Substring = require("./syntax/Substring");
// tests.ToType = require("./syntax/ToType");
// tests.CreateTrigger = require("./syntax/CreateTrigger");
tests.SqlFile = require("./syntax/SqlFile");
// tests.CommentOn = require("./syntax/CommentOn");

for (let className in tests) {
    describe(className, () => {
        tests[ className ].forEach(test => {
            testSyntax(className, test);
        });
    });
}