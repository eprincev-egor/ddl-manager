"use strict";

const Syntax = require("./Syntax");

class ObjectLink extends Syntax {
    parse(coach, options) {
        options = options || {availableStar: false};
        this.link = [];

        this.parseLink(coach, options);
    }

    parseLink(coach, options) {
        if ( options.availableStar && coach.is("*") ) {
            this.link.push( "*" );
            coach.i++;
        } else {
            let elem = coach.parseObjectName();
            this.addChild( elem );
            this.link.push( elem );
        }

        if ( coach.is(/\s*\./) ) {
            coach.skipSpace();
            coach.i++; // .
            coach.skipSpace();

            this.parseLink( coach, options );
        }
    }

    is(coach) {
        return coach.isDoubleQuotes() || coach.isWord();
    }

    isStar() {
        return this.link.some(elem => elem == "*");
    }

    last() {
        return this.link[ this.link.length - 1 ];
    }

    clone() {
        let clone = new ObjectLink();
        this.fillClone(clone);
        return clone;
    }

    fillClone(clone) {
        clone.link = this.link.map(elem => {
            if ( elem == "*" ) {
                return "*";
            }

            let elemClone = elem.clone();
            clone.addChild(elemClone);
            return elemClone;
        });
    }

    toString() {
        return this.link.map(elem => {
            if ( elem == "*" ) {
                return "*";
            }

            return elem.toString();
        }).join(".");
    }

    toLowerString() {
        return this.link.map(elem => {
            if ( elem == "*" ) {
                return "*";
            }

            return elem.toLowerCase();
        }).join(".");
    }

    getType(params) {
        let Select = this.Coach.Select;
        let select = this.findParentInstance(Select);

        if ( !select ) {
            throw new Error("ObjectLink must be inside Select");
        }

        let source = select.getColumnSource({
            node: params.node,
            server: params.server,
            link: this
        });
        if ( source.expression ) {
            return source.expression.getType(params);
        } else {
            let dbColumn = source.dbColumn;
            return dbColumn.type;
        }
    }

    add(name) {
        if ( !this.link ) {
            this.link = [];
        }

        if ( name != "*" ) {
            this.addChild(name);
        }
        this.link.push(name);
    }

    shift() {
        if ( !this.link || !this.link.length ) {
            return;
        }

        let elem = this.link.shift();

        let index = this.children.indexOf(elem);
        if ( index != -1 ) {
            this.children.splice(index, 1);
        }

        return elem;
    }

    unshift(elem) {
        this.link.unshift(elem);
        this.addChild(elem);
    }

    slice(from, to) {
        let subLink;

        if ( to != null ) {
            subLink = this.link.slice(from, to);
        } else {
            subLink = this.link.slice(from);
        }

        let clone = new ObjectLink();
        subLink.forEach(elem => clone.add(elem));

        return clone;
    }

    first() {
        return this.link[0];
    }

    containLink(objectLink) {
        if ( this.link.length < objectLink.link.length ) {
            return false;
        }

        for (let i = 0, n = objectLink.link.length; i < n; i++) {
            let myElem = this.link[i];
            let himElem = objectLink.link[i];

            if (
                myElem == "*" && himElem != "*"  ||
                myElem != "*" && himElem == "*"
            ) {
                return false;
            }

            if ( myElem == "*" && himElem == "*" ) {
                continue;
            }

            if ( !myElem.equal( himElem ) ) {
                return false;
            }
        }

        return true;
    }

    equalLink(objectLink) {
        if ( objectLink instanceof this.Coach.ObjectName ) {
            return (
                this.link.length == 1 &&
                this.lastEqual( objectLink )
            );
        }

        return (
            this.link.length == objectLink.link.length &&
            this.containLink( objectLink )
        );
    }

    lastEqual(objectName) {
        let lastElem = this.last();
        return lastElem.equal( objectName );
    }

    replace(replace, to) {
        replace = replace.link;

        if ( to instanceof this.Coach.ObjectName ) {
            to = [to];
        } else {
            to = to.link;
        }

        if ( this.link.length < replace.length ) {
            return;
        }

        let spliceLength = 0;
        for (let
            i = 0,
            n = this.link.length,
            m = replace.length;
            i < n && i < m;
            i++
        ) {
            let elem = this.link[i];
            let replaceElem = replace[i];

            if ( !elem.equal(replaceElem) ) {
                return;
            }
            spliceLength++;
        }

        for (let i = 0; i < spliceLength; i++) {
            this.link.shift();
        }

        for (let i = to.length - 1; i >= 0 ; i--) {
            this.link.unshift( to[i].clone() );
        }
    }

    clear() {
        this.children = [];
        this.link = [];
    }

    getDbTableLowerPath() {
        if ( this.link.length === 1 ) {
            return "public." + this.link[0].toLowerCase();
        } else {
            return this.link.map(name => name.toLowerCase()).join(".");
        }
    }

    getParentFunctionCall() {
        let functionCall;

        this.findParent(parent => {
            if ( parent instanceof this.Coach.Select ) {
                return true;
            }

            if ( parent instanceof this.Coach.FunctionCall ) {
                functionCall = parent;
                return true;
            }
        });

        return functionCall;
    }
}

module.exports = ObjectLink;
