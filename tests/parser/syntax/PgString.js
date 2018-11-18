"use strict";

module.exports = [
    {
        str: "'hello ''world'''",
        result: {content: "hello 'world'"}
    },
    {
        str: "'hello'\n'world'",
        result: {content: "helloworld"}
    },
    {
        str: "'hello'\r'world'",
        result: {content: "helloworld"}
    },
    {
        str: "'hello'\n\r'world'",
        result: {content: "helloworld"}
    },
    {
        str: "'hello' \n\r 'world' \n\r 'world'",
        result: {content: "helloworldworld"}
    },
    {
        str: "'hello' \n\r 'world' \n\r 'world'  ",
        result: {content: "helloworldworld"}
    },
    {
        str: "'hello' \n\r 'world' \n\r 'world' \n ",
        result: {content: "helloworldworld"}
    },
    {
        str: "'hello' \n\r 'world' \n\r 'world' \r ",
        result: {content: "helloworldworld"}
    },
    {
        str: "'hello' \n\r 'world' \n\r 'world' \n\r ",
        result: {content: "helloworldworld"}
    },
    {
        str: "E'\\\\'",
        result: {content: "\\"}
    },
    {
        str: "E'\\n'",
        result: {content: "\n"}
    },
    {
        str: "E'\\r'",
        result: {content: "\r"}
    },
    {
        str: "E'\\b'",
        result: {content: "\b"}
    },
    {
        str: "E'\\f'",
        result: {content: "\f"}
    },
    {
        str: "E'\\t'",
        result: {content: "\t"}
    },
    {
        str: "E'\\U000061b'",
        result: {content: "ab"}
    },
    {
        str: "E'\\U00006aa'",
        result: {content: "ja"}
    },
    {
        str: "E'\\U00006A'",
        result: {content: "j"}
    },
    {
        str: "E'\\u0061'",
        result: {content: "a"}
    },
    {
        str: "E'\\061a'",
        result: {content: "1a"}
    },
    {
        str: "U&'d\\0061t\\+000061 test'",
        result: {content: "data test"}
    },
    {
        str: "u&'d\\0061t\\+000061 test'",
        result: {content: "data test"}
    },
    {
        str: "U&'d!0061t!+000061' UESCAPE '!'",
        result: {content: "data"}
    },
    {
        str: "U&'\\006'",
        error: Error
    },
    {
        str: "b'01'",
        result: {content: "01"}
    },
    {
        str: "b''",
        result: {content: ""}
    },
    {
        str: "b'01a'",
        error: Error
    },
    {
        str: "x'af'",
        result: {content: "10101111"}
    },
    {
        str: "x'a'\n'f'",
        result: {content: "10101111"}
    },
    {
        str: "x''",
        result: {content: ""}
    },
    {
        str: "x'01x'",
        error: Error
    },
    {
        str: "$$hello'world$$",
        result: {content: "hello'world"}
    },
    {
        str: "$Tag_1$hello'world$Tag_1$",
        result: {content: "hello'world"}
    },
    {
        str: "$Tag_1$$tag_1$$Tag_1$",
        result: {content: "$tag_1$"}
    },
    {
        str: "$$\n\r$$",
        result: {content: "\n\r"}
    },
    {
        str: "$q$[\\t\\r\\n\\v\\\\]$q$",
        result: {content: "[\\t\\r\\n\\v\\\\]"}
    }
];
