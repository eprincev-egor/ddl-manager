
export const VOID_BODY = {
    content: `begin
end`
};

export function generateEmptyFunction(name: string) {
    return `
        create or replace function ${name}()
        returns void as $body$${VOID_BODY.content}$body$
        language plpgsql;
    `.trim();
}