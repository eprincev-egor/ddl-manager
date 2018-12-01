"use strict";

const Syntax = require("./Syntax");
// TODO: load types from db
const types = [
    "smallint",
    "integer",
    "bigint",
    "decimal",
    "numeric(n)",
    "numeric(n,n)",
    "real",
    "double precision",
    "smallserial",
    "serial",
    "bigserial",
    "money",
    "character varying(n)",
    "varchar(n)",
    "character(n)",
    "char(n)",
    "text",
    "\"char\"",
    "name",
    "bytea",
    // don't touch order ! see 'posibleTypes' checks
    "timestamp without time zone",
    "timestamp with time zone",
    "timestamp",
    "time without time zone",
    "time with time zone",
    // ...
    "boolean",
    "point",
    "line",
    "lseg",
    "box",
    "path",
    "polygon",
    "path",
    "circle",
    "cidr",
    "inet",
    "macaddr",
    "macaddr8",
    "bit(n)",
    "bit varying(n)",
    "tsvector",
    "tsquery",
    "uuid",
    "xml",
    "json",
    "jsonb",
    "int",
    "int4range",
    "int8range",
    "numrange",
    "tsrange",
    "tstzrange",
    "daterange",
    "regclass",
    "regproc",
    "regprocedure",
    "regoper",
    "regoperator",
    "regclass",
    "regtype",
    "regrole",
    "regnamespace",
    "regconfig",
    "regdictionary",
    "date",
    "trigger",
    "void"
];

let firstWords = {};
types.forEach(type => {
    let firstWord = type.split(" ")[0];
    firstWord = firstWord.split("(")[0];
    
    if ( !firstWords[firstWord] ) {
        firstWords[firstWord] = [];
    }
    firstWords[ firstWord ].push( type );
});

let regExps = {};
types.forEach(type => {
    let regExp = type.replace(/ /g, "\\s+");
    regExp = regExp.replace("(n)", "\\s*\\(\\s*\\d+\\s*\\)");
    regExp = regExp.replace("(n,n)", "\\s*\\(\\s*\\d+\\s*,\\s*\\d+\\s*\\)");
    
    regExps[ type ] = new RegExp(regExp, "i");
});

class DataType extends Syntax {
    parse(coach) {
        if ( coach.is("\"") ) {
            coach.i++;
            coach.expectWord("char");
            coach.expect("\"");
            this.type = "\"char\"";
            return;
        }
        
        let index = coach.i;
        let word = coach.readWord().toLowerCase();
        let availableTypes = firstWords[ word ] || [];
        
        coach.i = index;
        for (let i = 0, n = availableTypes.length; i < n; i++) {
            let availableType = availableTypes[ i ];
            let regExp = regExps[ availableType ];
            
            if ( coach.is(regExp) ) {
                this.type = regExp.exec( coach.str.slice(coach.i) )[0];
                coach.i += this.type.length;
                
                this.type = this.type.replace(/\s+/g, " ");
                this.type = this.type.replace(/\s*\(\s*/g, "(");
                this.type = this.type.replace(/\s*\)\s*/g, ")");
                this.type = this.type.replace(/\s*,\s*/g, ",");
                this.type = this.type.toLowerCase();
                break;
            }
        }
        
        if ( !this.type ) {
            if ( word == "numeric" ) {
                this.type = "numeric";
                coach.i += this.type.length;
            } 
            else {
                let {schema, name} = coach.parseSchemaName();
                this.type = schema + "." + name;
            }
        }
        
        this.parseArrayType(coach);
    }
    
    parseArrayType(coach) {
        if ( coach.is(/\s*\[/) ) {
            coach.skipSpace();
            coach.i++;
            coach.skipSpace();
            
            if ( coach.is("]") ) {
                coach.i++;
                this.type += "[]";
            } else {
                let pgNumb = coach.parsePgNumber();
                coach.skipSpace();
                coach.expect("]");
                
                this.type += "[" + pgNumb.number + "]";
            }
        }
        
        if ( coach.is(/\s*\[/) ) {
            this.parseArrayType(coach);
        }
    }
    
    is(coach) {
        let i = coach.i;
        let word = coach.readWord().toLowerCase();
        coach.i = i;
        return word in firstWords;
    }
    
    clone() {
        let clone = new DataType();
        clone.type = this.type;
        return clone;
    }
    
    toString() {
        return this.type;
    }
}

module.exports = DataType;
