// One error type for every refusal: its message is what the operator
// reads, and its code is the process exit code (1 refused or failed,
// 2 usage, 3 "not yet: publish or wait, then run the same command again").
// No message ever carries key material or a token.
export class AdminError extends Error {
  constructor(
    message: string,
    readonly code: 1 | 2 | 3 = 1,
  ) {
    super(message);
  }
}

/// Exit code 3: the step is safe to repeat once the operator acts.
export const notYet = (message: string) => new AdminError(message, 3);
