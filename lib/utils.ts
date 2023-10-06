
export function fixErrorStack(
    sql: string,
    originalErr: any,
    stack: string | undefined
): Error {
    const correctError = new Error(originalErr.message) as any;
    // system info (postgres stack, codes, ...)
    Object.assign(correctError, originalErr);

    // redefine call stack
    correctError.stack = `
${originalErr.message}

${originalErr.where || ""}

Failed query:
${sql}

    ${stack?.replace("Error:", "")}
    `.trim();

    return correctError;
}

export async function sleep(ms: number) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}
