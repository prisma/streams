import type { TouchConfig } from "../../touch/spec";

export type StateProtocolStreamProfile = {
  kind: "state-protocol";
  touch?: TouchConfig;
};
