
// timestamp for file name
export function date2timestamp(date: Date) {

    let day: string = date.getDay().toString();
    let month: string = (date.getMonth() + 1).toString();
    const year = date.getFullYear().toString();
    let hours: string = date.getHours().toString();
    let minutes: string = date.getMinutes().toString();
    let seconds: string = date.getSeconds().toString();

    if ( +day < 10 ) {
        day = "0" + day;
    }
    if ( +month < 10 ) {
        month = "0" + month;
    }

    if ( +hours < 10 ) {
        hours = "0" + hours;
    }
    if ( +minutes < 10 ) {
        minutes = "0" + minutes;
    }
    if ( +seconds < 10 ) {
        seconds = "0" + seconds;
    }

    // cannot use symbols for file name  :|?*<>/\"
    return `${day}.${month}.${year}__${hours}.${minutes}.${seconds}`;
}