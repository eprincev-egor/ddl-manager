"use strict";

class Coach {
    constructor(str) {
        this.str = str;
        this.n = str.length;
        this.i = 0;
    }

    throwError(message, index) {
        let position = this.getPosition(index);

        throw new Error(
            "\n SyntaxError at line " + (position.line + 1) +
                ", position " + (position.column + 1) +
                ", at near `" + this.str.slice(Math.max(position.index, 0), position.index + 30) + "`" +
                "\n Message: " + message
        );
    }

    skipSpace() {
        for (; this.i < this.n; this.i++) {
            let symbol = this.str[ this.i ];

            if ( !/\s/.test(symbol) ) {
                break;
            }
        }
    }

    expectWord(word) {
        let currentWord = this.readWord();

        if ( word == null ) {
            if ( !currentWord ) {
                this.throwError("expected any word");
            }
            return currentWord;
        }

        if ( currentWord.toLowerCase() == word ) {
            return currentWord;
        }

        this.throwError("expected word: " + word);
    }

    expect(strOrRegExp, message) {
        if ( typeof strOrRegExp == "string" ) {
            let str = strOrRegExp;

            if ( this.str.slice(this.i).indexOf(str) === 0 ) {
                this.i += str.length;
            } else {
                if ( message == null ) {
                    message = "expected: " + str;
                }
                this.throwError(message);
            }

            return str;
        } else {
            let regExp = strOrRegExp;
            let str = this.str.slice(this.i);
            let execResult = regExp.exec(str);

            if ( !execResult || execResult.index !== 0 ) {
                if ( message == null ) {
                    message = "expected: " + regExp;
                }
                this.throwError(message);
            }

            this.i += execResult[0].length;
            return execResult[0];
        }
    }

    readWord() {
        let word = "";

        for (; this.i < this.n; this.i++) {
            let symbol = this.str[ this.i ];

            if ( /[^\w]/.test(symbol) ) {
                break;
            }

            word += symbol;
        }

        return word;
    }

    read(regExp) {
        let str = this.str.slice(this.i);
        let execResult = regExp.exec(str);

        if ( !execResult || execResult.index !== 0 ) {
            return "";
        }

        this.i += execResult[0].length;
        return execResult[0];
    }

    parseUnicode(unicode) {
        try {
            // unicode can be valid js code
            if ( !/^[\dabcdef]+$/i.test(unicode) ) {
                throw new Error();
            }

            unicode = eval("'\\u{" + unicode + "}'");
        } catch(err) {
            this.throwError("invalid unicode sequence: " + unicode);
        }

        return unicode;
    }

    isWord(word) {
        if ( word == null ) {
            return this.is(/\w/i);
        }

        let i = this.i;
        let currentWord = this.readWord();
        this.i = i;

        return currentWord.toLowerCase() == word;
    }

    // test string (from current place) on regExp
    is(regExp) {
        let str = this.str.slice(this.i);

        if (
            typeof regExp === "function" &&
            typeof regExp.prototype.is === "function"
        ) {
            return new regExp().is(this, str);
        }

        else if ( typeof regExp === "string" ) {
            return str.indexOf(regExp) === 0;
        }

        else {
            return str.search(regExp) === 0;
        }
    }

    isEnd() {
        return this.i >= this.str.length;
    }

    // parsing over "," and returning array of syntax objects
    // first argument SyntaxName is string: "Expression" or "ObjectLink" or any SyntaxName,
    // first symbol must be in upper case
    // or object like are:
    // {
    //    is: function,
    //    parse: function
    // }
    parseComma(SyntaxName, options) {
        let elements = [];
        let parseSyntax;
        let isSyntax;

        if ( typeof SyntaxName == "string" ) {
            if ( options ) {
                parseSyntax = this[ "parse" + SyntaxName ].bind(this, options);
            } else {
                parseSyntax = this[ "parse" + SyntaxName ].bind(this);
            }

            isSyntax = this[ "is" + SyntaxName ].bind(this);
        } else {
            parseSyntax = SyntaxName.parse;
            isSyntax = SyntaxName.is;
        }

        this._parseComma(isSyntax, parseSyntax, elements);

        return elements;
    }

    _parseComma(isSyntax, parseSyntax, elements) {
        let elem;

        if ( isSyntax() ) {
            elem = parseSyntax();
            elements.push( elem );

            if ( this.is(/\s*,/) ) {
                this.skipSpace();
                this.i++; // ,
                this.skipSpace();

                this._parseComma(isSyntax, parseSyntax, elements);
            }
        }

        return elements;
    }

    // parsing chain of syntax objects separated by space symbols
    // first argument SyntaxName is string: "Expression" or "ObjectLink" or any SyntaxName,
    // first symbol must be in upper case
    // or object like are:
    // {
    //    is: function,
    //    parse: function
    // }
    parseChain(SyntaxName) {
        let elements = [];
        let parseSyntax;
        let isSyntax;

        if ( typeof SyntaxName == "string" ) {
            parseSyntax = this[ "parse" + SyntaxName ].bind(this);
            isSyntax = this[ "is" + SyntaxName ].bind(this);
        } else {
            parseSyntax = SyntaxName.parse;
            isSyntax = SyntaxName.is;
        }

        this._parseChain(isSyntax, parseSyntax, elements);

        return elements;
    }

    _parseChain(isSyntax, parseSyntax, elements) {
        let i = this.i;
        this.skipSpace();

        if ( isSyntax() ) {
            let elem = parseSyntax();
            elements.push( elem );

            this._parseChain(isSyntax, parseSyntax, elements);
        } else {
            this.i = i;
        }

        return elements;
    }

    getPosition(index) {
        let position = +index === +index ? +index : this.i; // index type maybe string or number, but not NaN
        let lines = this.str.slice(0, position).split("\n");
        let lineNumber = lines.length;
        let currentLine = lines.pop();
        let column = position - lines.join("\n").length - (currentLine.length - currentLine.replace(/^[\n\r]+/, "").length); // POP!

        return {
            index: position,
            line: lineNumber - 1,
            column: column - 1
        };
    }
}

Coach.addSyntax = function(className, SyntaxClass) {
    let ChildCoach = this;

    ChildCoach[ className ] = SyntaxClass;
    SyntaxClass.prototype.Coach = ChildCoach;

    let funcName;

    if ( SyntaxClass.prototype.is ) {
        funcName = "is" + className;
        ChildCoach.prototype[funcName] = function isSyntax() {
            return this.is(SyntaxClass);
        };
    }

    funcName = "parse" + className;
    ChildCoach.prototype[funcName] = function parseSyntax() {
        let args = [this].concat([].slice.call(arguments));
        let syntax = new SyntaxClass();

        syntax.coach = this;
        syntax.startIndex = this.i;
        syntax.parse.apply(syntax, args);
        syntax.endIndex = this.i;

        return syntax;
    };
};

module.exports = Coach;
