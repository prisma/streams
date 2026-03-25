export type CanonicalChange = {
  entity: string;
  key?: string;
  op: "insert" | "update" | "delete";
  before?: unknown;
  after?: unknown;
};
