import type { ChangeEvent, ControlEvent } from "@durable-streams/state";

type Assert<T extends true> = T;
type AssertFalse<T extends false> = T;
type IsAssignable<From, To> = [From] extends [To] ? true : false;

type PrismaWalChangeEvent = {
  type: "public.posts";
  key: "42";
  value: { id: number; title: string };
  old_value: { id: number; title: string };
  headers: {
    operation: "update";
    txid: string;
    timestamp: string;
  };
};

type _PrismaWalChangeEventMatchesStateProtocol = Assert<
  IsAssignable<PrismaWalChangeEvent, ChangeEvent<{ id: number; title: string }>>
>;

type _StateProtocolUsesSnakeCaseBeforeImage = Assert<"old_value" extends keyof ChangeEvent<unknown> ? true : false>;
type _StateProtocolDoesNotUseCamelCaseBeforeImage = AssertFalse<"oldValue" extends keyof ChangeEvent<unknown> ? true : false>;

type PrismaWalControlEvent = {
  headers: {
    control: "reset";
    offset: string;
  };
};

type _PrismaWalControlEventMatchesStateProtocol = Assert<
  IsAssignable<PrismaWalControlEvent, ControlEvent>
>;
