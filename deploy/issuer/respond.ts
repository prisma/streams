// The issuer's answers: JSON, never cached, errors in the product envelope
// `{"error":{"code","message"}}` the cell and the SDK already speak
// (WIRE-MATRIX §0). No answer carries a CORS header: a durable secret must
// not reach a browser, so no browser may read the exchange's answer.
export function json(status: number, body: unknown, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store", ...headers },
  });
}

export const refuse = (status: number, code: string, message: string, headers: Record<string, string> = {}) =>
  json(status, { error: { code, message } }, headers);
