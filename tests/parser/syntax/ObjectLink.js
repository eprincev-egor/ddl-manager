"use strict";

module.exports = [
    {
        str: "a.B.c",
        result: {
            link: [
                {word: "a"},
                {word: "B"},
                {word: "c"}
            ]
        }
    },
    {
        str: "a.b.c.d.e.f",
        result: {
            link: [
                {word: "a"},
                {word: "b"},
                {word: "c"},
                {word: "d"},
                {word: "e"},
                {word: "f"}
            ]
        }
    },
    {
        str: `"Nice"
            .
            "test"   . X.y."some"
            `,
        result: {
            link: [
                {content: "Nice"},
                {content: "test"},
                {word: "X"},
                {word: "y"},
                {content: "some"}
            ]
        }
    }
];
