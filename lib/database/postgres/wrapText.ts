
export function wrapText(text: string, tag: string = "tag") {
    text += "";

    if ( !text.includes(`$${tag}$`) ) {
        return `$${tag}$${ text }$${tag}$`;
    }

    let index = 1;
    while ( text.includes("$tag" + index + "$") ) {
        index++;
    }
    tag += index;

    return `$${tag}$${ text }$${tag}$`;
}
