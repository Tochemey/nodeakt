import { describe } from "vitest";
import { UnboundedMailbox } from "../src/actor/unbounded.mailbox";
import { describeFifo, describeMailboxContract } from "./mailbox.contract";

describe("UnboundedMailbox", () => {
  describeMailboxContract(() => new UnboundedMailbox());
  describeFifo(() => new UnboundedMailbox());
});
