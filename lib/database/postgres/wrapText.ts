
export function wrapText(text: string) {
    text += "";
    let tag = "tag";
    let index = 1;
    while ( text.indexOf("$tag" + index + "$") !== -1 ) {
        index++;
    }
    tag += index;

    return `$${tag}$${ text }$${tag}$`;
}
