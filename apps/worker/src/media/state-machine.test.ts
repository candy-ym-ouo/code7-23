import { describe, expect, it } from "vitest";
import { assertTransition, canTransition, isRecoverableInFlight, isTerminal } from "./state-machine";

describe("media state machine", () => {
  it("permits the documented happy and recovery edges", () => {
    expect(canTransition("quarantined", "processing")).toBe(true);
    expect(canTransition("processing", "scanning")).toBe(true);
    expect(canTransition("scanning", "processing")).toBe(true);
    expect(canTransition("processing", "ready")).toBe(true);
    expect(canTransition("processing", "manual_review")).toBe(true);
    expect(canTransition("manual_review", "ready")).toBe(true);
  });

  it("collapses any in-flight state to failed and lets failed retry", () => {
    expect(canTransition("scanning", "failed")).toBe(true);
    expect(canTransition("processing", "failed")).toBe(true);
    expect(canTransition("failed", "scanning")).toBe(true);
  });

  it("rejects jumps that skip the pipeline", () => {
    expect(canTransition("quarantined", "ready")).toBe(false);
    expect(canTransition("scanning", "ready")).toBe(false);
    expect(canTransition("ready", "manual_review")).toBe(false);
    expect(canTransition("failed", "ready")).toBe(false);
    expect(canTransition("deleted", "ready")).toBe(false);
  });

  it("treats self-transitions as idempotent replays", () => {
    expect(canTransition("ready", "ready")).toBe(true);
    expect(() => assertTransition("deleted", "deleted")).not.toThrow();
    expect(() => assertTransition("ready", "failed")).toThrow(/Illegal/);
  });

  it("classifies terminal and recoverable states", () => {
    expect(isTerminal("ready")).toBe(true);
    expect(isTerminal("manual_review")).toBe(true);
    expect(isTerminal("deleted")).toBe(true);
    expect(isTerminal("processing")).toBe(false);
    expect(isRecoverableInFlight("scanning")).toBe(true);
    expect(isRecoverableInFlight("processing")).toBe(true);
    expect(isRecoverableInFlight("failed")).toBe(false);
  });
});
