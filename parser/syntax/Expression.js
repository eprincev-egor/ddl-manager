"use strict";

const Syntax = require("./Syntax");

class Expression extends Syntax {
    constructor(fromString) {
        super();
        this.elements = [];

        this.fromString(fromString);
    }

    parse(coach, options) {
        options = options || {availableStar: false, excludeOperators: false};

        this.parseElements( coach, options );
        this.elements = this.extrude(this.elements);
    }

    is(coach) {
        // for stopping parseComma
        return !coach.isEnd() && !coach.is(/[\s),]/);
    }

    parseElements(coach, options) {
        let elem;
        let i;
        let result;

        if ( options.availableStar && coach.is("*") ) {
            elem = coach.parseColumnLink({ availableStar: options.availableStar });
            this.addChild(elem);
            this.elements.push(elem);
            return;
        }

        result = this.parseOperators(coach, options);
        if ( result === false ) {
            return;
        }

        i = coach.i;
        elem = this.parseElement( coach, options );
        if ( !elem ) {
            coach.i = i;
            coach.throwError("expected expression element");
        }
        this.addChild(elem);
        this.elements.push(elem);

        // company.id in (1,2)
        // company.id between 1 and 2
        coach.skipSpace();
        if ( coach.isBetween() || coach.isIn() ) {
            elem = this.parseElement( coach, options );
            this.addChild(elem);
            this.elements.push(elem);
        }

        if ( coach.isSquareBrackets() ) {
            let brackets = coach.parseSquareBrackets();
            this.elements.push(brackets);
            this.addChild(elem);
        }

        // ::text::text::text
        this.parseToTypes(coach);

        // operator
        coach.skipSpace();
        if ( coach.isOperator() ) {
            let result = this.parseOperators(coach, options);
            if ( result === false ) {
                return;
            }
            let lastOperator = this.elements.slice(-1)[0];

            // fix for:
            // default 0 not null
            if ( lastOperator.operator == "not" ) {
                let coachPosition = lastOperator.startIndex;
                let operatorIndex = this.elements.length - 1;

                this.parseElements(coach, options);

                let elem = this.elements[ operatorIndex + 1 ];

                let isValidElem = !(elem instanceof this.Coach.PgNull);

                if ( !isValidElem ) {
                    coach.i = coachPosition;
                    this.elements
                        .splice(operatorIndex)
                        .forEach(elem => this.removeChild(elem));
                }
            }

            else {
                this.parseElements(coach, options);
            }
        }
    }

    parseOperators(coach, options) {
        if ( coach.isOperator() ) {
            let i = coach.i;

            let operator = coach.parseOperator();

            if ( options.excludeOperators ) {
                if ( options.excludeOperators.includes(operator.operator) ) {
                    coach.i = i;
                    return false;
                }
            }

            this.addChild(operator);
            this.elements.push( operator );
            coach.skipSpace();

            this.parseOperators(coach, options);
        }
    }

    parseElement(coach, options) {
        let elem;

        // sub expression
        if ( coach.is("(") ) {
            coach.i++;
            coach.skipSpace();

            // ту мог быть select, но мы используем expression только для default в аргументах функции
            // а его там не может быть
            elem = coach.parseExpression();

            coach.skipSpace();
            coach.expect(")");
        }

        else if ( coach.isCast() ) {
            elem = coach.parseCast();
        }

        else if ( coach.isIn() ) {
            elem = coach.parseIn();
        }

        else if ( coach.isExtract() ) {
            elem = coach.parseExtract();
        }
        
        else if ( coach.isSubstring() ) {
            elem = coach.parseSubstring();
        }

        else if ( coach.isAny() ) {
            elem = coach.parseAny();
        }

        else if ( coach.isBetween() ) {
            elem = coach.parseBetween();
        }

        else if ( coach.isPgNull() ) {
            elem = coach.parsePgNull();
        }

        else if ( coach.isBoolean() ) {
            elem = coach.parseBoolean();
        }

        else if ( coach.isPgNumber() ) {
            elem = coach.parsePgNumber();
        }

        else if ( coach.isPgString() ) {
            elem = coach.parsePgString();
        }

        else if ( coach.isCaseWhen() ) {
            elem = coach.parseCaseWhen();
        }

        else if ( coach.isFunctionCall() ) {
            elem = coach.parseFunctionCall();
        }

        else if ( coach.isPgArray() ) {
            elem = coach.parsePgArray();
        }

        else if ( coach.isColumnLink() ) {
            elem = coach.parseColumnLink({ availableStar: options.availableStar });
        }

        return elem;
    }

    parseToTypes(coach) {
        if ( coach.isToType() ) {
            let elem = coach.parseToType();
            this.addChild(elem);
            this.elements.push( elem );

            coach.skipSpace();
            this.parseToTypes(coach);
        }
    }

    replaceElement(element, to) {
        let index = this.elements.indexOf(element);
        if ( index == -1 ) {
            return;
        }

        this.removeChild(element);
        
        if ( typeof to === "string" ) {
            to = new Expression(to);
            to = to.elements[0];
        }
        
        this.elements.splice(index, 1, to);
        this.addChild(to);
    }

    // ((( expression )))  strip unnecessary brackets
    extrude(elements) {
        if ( elements.length === 1 && elements[0] instanceof Expression ) {
            return this.extrude( elements[0].elements );
        }
        return elements;
    }

    isLink() {
        return this.elements.length === 1 && !!this.elements[0].link;
    }

    getLink() {
        return this.elements[0];
    }

    clone() {
        let clone = new Expression();
        clone.elements = this.elements.map(elem => elem.clone());
        clone.elements.map(elem => clone.addChild(elem));
        return clone;
    }

    toString() {
        let out = "";

        this.elements.forEach((elem, i) => {
            if ( i > 0 ) {
                out += " ";
            }

            if ( elem instanceof Expression ) {
                out += "( ";
                out += elem.toString();
                out += " )";
            } else {
                out += elem.toString();
            }
        });

        return out;
    }
}

module.exports = Expression;
