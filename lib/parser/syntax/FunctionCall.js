"use strict";

const Syntax = require("./Syntax");

// some(1,2)

class FunctionCall extends Syntax {
    parse(coach) {
        this.function = coach.parseFunctionLink();
        this.addChild(this.function);

        coach.skipSpace();
        coach.expect("(");
        coach.skipSpace();

        this.arguments = coach.parseComma("Expression");
        this.arguments.map(arg => this.addChild(arg));

        coach.skipSpace();

        coach.skipSpace();
        coach.expect(")");
        coach.skipSpace();
    }

    is(coach) {
        let i = coach.i;
        let result = false;

        try {
            coach.parseFunctionLink();
            coach.skipSpace();
            result = coach.is("(");
        } catch(err) {
            result = false;
        }

        coach.i = i;
        return result;
    }

    clone() {
        let clone = new FunctionCall();

        clone.function = this.function.clone();
        clone.addChild(this.function);

        clone.arguments = this.arguments.map(arg => arg.clone());
        clone.arguments.map(arg => clone.addChild(arg));

        return clone;
    }

    toString() {
        let out = "";

        out += this.function.toString();
        out += "(";

        out += this.arguments.map(arg => arg.toString()).join(", ");

        out += ")";

        return out;
    }
}

module.exports = FunctionCall;
