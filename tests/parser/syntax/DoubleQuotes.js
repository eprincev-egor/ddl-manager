"use strict";

module.exports = [
    {
        str: "\"test\"",
        result: {content: "test"}
    },
    {
        str: "\"test\"\"\"",
        result: {content: "test\""}
    },
    {
        str: "U&\"d\\0061t\\+000061 test\"",
        result: {content: "data test"}
    },
    {
        str: "u&\"d\\0061t\\+000061 test\"",
        result: {content: "data test"}
    },
    {
        str: "U&\"d!0061t!+000061\" UESCAPE '!'",
        result: {content: "data"}
    },
    {
        str: "U&\"\\006\"",
        error: Error
    },
    {
        str: "U&\"\\006ф\"",
        error: Error
    },
    {
        str: "\"\" uescape '!'",
        error: Error
    },
    {
        str: "u&\"\" uescape '+'",
        error: Error
    },
    {
        str: "u&\"\" uescape '-'",
        error: Error
    },
    {
        str: "u&\"\" uescape '''",
        error: Error
    },
    {
        str: "u&\"\" uescape '\"'",
        error: Error
    },
    {
        str: "u&\"\" uescape ' '",
        error: Error
    },
    {
        str: "u&\"\" uescape '\n'",
        error: Error
    },
    {
        str: "u&\"\" uescape '\t'",
        error: Error
    },
    {
        str: "u&\"\" uescape 'a'",
        error: Error
    },
    {
        str: "u&\"\" uescape 'b'",
        error: Error
    },
    {
        str: "u&\"\" uescape 'c'",
        error: Error
    },
    {
        str: "u&\"\" uescape 'd'",
        error: Error
    },
    {
        str: "u&\"\" uescape 'e'",
        error: Error
    },
    {
        str: "u&\"\" uescape 'f'",
        error: Error
    },
    {
        str: "u&\"\" uescape '0'",
        error: Error
    },
    {
        str: "u&\"\" uescape '9'",
        error: Error
    }
];
