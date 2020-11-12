
export const VOID_BODY = `begin
end`;

export function generateEmptyFunction(name: string) {
    return `
        create or replace function ${name}()
        returns void as $body$${VOID_BODY}$body$
        language plpgsql;
    `.trim();
}