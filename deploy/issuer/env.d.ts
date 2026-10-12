// A `.env` profile imported `with { type: "text" }` (profiles.ts): Bun
// inlines the file's text.
declare module "*.env" {
  const text: string;
  export default text;
}
