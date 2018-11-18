"use strict";

const Syntax = require("./Syntax");

class PgString extends Syntax {
    parse(coach) {
        let startIndex = coach.i;

        if ( coach.is(/[xb]/i) ) {
            this.parseByteString(coach);
            // easy way for .toString()
            this._source = coach.str.slice(startIndex, coach.i);
            return;
        }

        if ( coach.is("$") ) {
            this.parseDollarString(coach);
            // easy way for .toString()
            this._source = coach.str.slice(startIndex, coach.i);
            return;
        }

        let withEscape = false,
            withUEscape = false;

        if ( coach.is(/e/i) ) {
            withEscape = true;
            coach.i++;
        }
        else if ( coach.is(/u&/i) ) {
            withUEscape = true;
            coach.i += 2;
        }

        let content = this.readSingleQuotes( coach, withEscape );

        let escape = "\\";
        if ( coach.is(/\s*uescape/i) ) {
            if ( !withUEscape ) {
                coach.throwError("unexpected uescape, use u& before double quotes  ");
            }

            coach.skipSpace();
            coach.expectWord("uescape");
            coach.skipSpace();

            coach.expect("'");
            escape = coach.str[ coach.i ];

            if ( /[+-\s\dabcdef"']/.test(escape) ) {
                coach.throwError("The escape character can be any single character other than a hexadecimal digit, the plus sign, a single quote, a double quote, or a whitespace character");
            }
            coach.i++;
            coach.expect("'");
        }

        if ( withUEscape ) {
            for (let i = 0, n = content.length; i < n; i++) {
                let symbol = content[i],
                    length;

                if ( symbol == escape ) {
                    let expr;
                    if ( content[i + 1] == "+" ) {
                        length = 8;
                        expr = content.slice(i + 2, i + length);
                    } else {
                        length = 5;
                        expr = content.slice(i + 1, i + length);
                    }

                    expr = coach.parseUnicode(expr);
                    n -= (length - 1);
                    content = content.slice(0, i) + expr + content.slice(i + length);
                }
            }
        }

        this.content = content;
        // easy way for .toString()
        this._source = coach.str.slice(startIndex, coach.i);
    }

    parseDollarString(coach) {
        let tag = "";

        coach.expect("$");
        if ( coach.is(/\w/) ) {
            tag = coach.readWord();
        }
        coach.expect("$");

        let content = "";
        for (; coach.i < coach.n; coach.i++) {
            let symbol = coach.str[ coach.i ];

            if ( symbol == "$" ) {
                let close = coach.str.slice( coach.i, coach.i + tag.length + 2 );

                if ( close == "$" + tag + "$" ) {
                    coach.i += tag.length + 2;
                    break;
                }
            }

            content += symbol;
        }

        this.content = content;
    }

    parseByteString(coach) {
        if ( coach.is(/b/i) ) {
            coach.i++;
            let content = this.readSingleQuotes(coach, false);

            if ( !/^[01]*$/.test(content) ) {
                coach.throwError("byte string b'' must contain only 0 or 1");
            }

            this.content = content;
        }

        else if ( coach.is(/x/i) ) {
            coach.i++;
            let content = this.readSingleQuotes(coach, false);

            if ( !/^[\dabcdef]*$/.test(content) ) {
                coach.throwError("byte string x'' must contain only digits or abcdef");
            }

            this.content = content.split("").map(x => parseInt(x, 16).toString(2)).join("");
        }
    }

    readSingleQuotes(coach, withEscape) {
        coach.expect("'");
        let content = "";

        for (; coach.i < coach.n; coach.i++) {
            let symbol = coach.str[ coach.i ];

            if ( symbol == "\\" && withEscape ) {
                content += this.readEscape(coach);
                coach.i--;
                continue;
            }

            if ( symbol == "'" ) {
                if ( coach.str[ coach.i + 1 ] == "'" ) {
                    content += "'";
                    coach.i++;
                    continue;
                }

                coach.i++;
                break;
            }

            content += symbol;
        }

        if ( coach.is(/(\s*[\n\r]+\s*)+'/) ) {
            coach.skipSpace();
            content += this.readSingleQuotes( coach );
        }

        return content;
    }

    readEscape(coach) {
        coach.i++;
        let symbol = coach.str[ coach.i ];

        if (
            symbol == "\\" ||
            symbol == "b" ||
            symbol == "f" ||
            symbol == "n" ||
            symbol == "r" ||
            symbol == "t"
        ) {
            coach.i++;
            return eval("'\\" + symbol + "'");
        }
        else if ( symbol == "x" ) {
            let h1 = coach.str[ coach.i + 1 ];
            let h2 = coach.str[ coach.i + 2 ];

            if( !/[\dabcdef]/.test(h1) ) {
                coach.throwError("invalid unicode sequence");
            }

            if ( /[\dabcdef]/.test(h2) ) {
                coach.i += 2;
                symbol = h1 + h2;
            } else {
                coach.i++;
                symbol = h1;
            }

            symbol = coach.parseUnicode(symbol);
            return symbol;
        }
        else if ( symbol == "u" ) {
            symbol = coach.str.slice(coach.i + 1, coach.i + 5);
            symbol = coach.parseUnicode(symbol);
            coach.i += 5;
            return symbol;
        }
        else if ( symbol == "U" ) {
            symbol = coach.str.slice(coach.i + 1, coach.i + 7);
            symbol = coach.parseUnicode(symbol);
            coach.i += 7;
            return symbol;
        }
        else if ( /[01234567]/.test(symbol) ) {
            coach.i++;

            if ( /[01234567]/.test(coach.str[ coach.i ]) ) {
                symbol += coach.str[ coach.i ];
                coach.i++;
            }
            if ( /[01234567]/.test(coach.str[ coach.i ]) ) {
                symbol += coach.str[ coach.i ];
                coach.i++;
            }

            symbol = parseInt( symbol, 8 );
            symbol = symbol.toString(16);
            symbol = coach.parseUnicode(symbol);

            return symbol;
        }

        return symbol;
    }

    is(coach, str) {
        return (
            str[0] == "'" ||

            (str[0] == "e" || str[0] == "E") &&
            str[1] == "'" ||

            (str[0] == "u" || str[0] == "U") &&
            str[1] == "&" &&
            str[2] == "'" ||

            (str[0] == "b" || str[0] == "B") &&
            str[1] == "'" ||

            (str[0] == "x" || str[0] == "X") &&
            str[1] == "'" ||

            coach.is(/\$\w*\$/)
        );
    }

    clone() {
        let clone = new PgString();
        clone.content = this.content;
        clone._source = this._source;
        return clone;
    }

    toString() {
        return this._source;
    }

    getType() {
        return "text";
    }
}

module.exports = PgString;
