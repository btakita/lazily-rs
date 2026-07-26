import { describe, expect, it } from "vitest";
import antiSpoofFixture from "../../../lazily-spec/conformance/signaling/anti_spoof_session.json";
import framesFixture from "../../../lazily-spec/conformance/signaling/frames.json";
import {
  decodeClientFrame,
  decodeServerFrame,
  encodeServerMessage,
  isPeerId,
  parseClientMessage,
  type ServerMessage,
} from "../src/protocol.js";

describe("isPeerId", () => {
  it("accepts safe non-negative integers", () => {
    expect(isPeerId(0)).toBe(true);
    expect(isPeerId(42)).toBe(true);
    expect(isPeerId(Number.MAX_SAFE_INTEGER)).toBe(true);
  });
  it("rejects unsafe, negative, or non-integer values", () => {
    expect(isPeerId(-1)).toBe(false);
    expect(isPeerId(1.5)).toBe(false);
    expect(isPeerId(Number.MAX_SAFE_INTEGER + 1)).toBe(false);
    expect(isPeerId("7")).toBe(false);
    expect(isPeerId(null)).toBe(false);
  });
});

describe("parseClientMessage", () => {
  it("parses a join with and without capabilities", () => {
    expect(parseClientMessage({ type: "join", peer: 1 })).toEqual({
      type: "join",
      peer: 1,
    });
    expect(
      parseClientMessage({ type: "join", peer: 1, capabilities: ["crdt"] }),
    ).toEqual({ type: "join", peer: 1, capabilities: ["crdt"] });
  });

  it("rejects join with bad peer or capabilities", () => {
    expect(parseClientMessage({ type: "join", peer: -1 })).toBeNull();
    expect(
      parseClientMessage({ type: "join", peer: 1, capabilities: [1] }),
    ).toBeNull();
  });

  it("parses offer/answer/ice/relay with targets", () => {
    expect(parseClientMessage({ type: "offer", to: 2, sdp: "x" })).toEqual({
      type: "offer",
      to: 2,
      sdp: "x",
    });
    expect(parseClientMessage({ type: "ice", to: 2, candidate: "c" })).toEqual({
      type: "ice",
      to: 2,
      candidate: "c",
    });
    expect(
      parseClientMessage({ type: "relay", to: 2, payload: { d: 1 } }),
    ).toEqual({ type: "relay", to: 2, payload: { d: 1 } });
  });

  it("rejects directed frames missing fields", () => {
    expect(parseClientMessage({ type: "offer", to: 2 })).toBeNull();
    expect(parseClientMessage({ type: "relay", to: 2 })).toBeNull();
    expect(parseClientMessage({ type: "ice", candidate: "c" })).toBeNull();
  });

  it("parses leave and rejects unknown/garbage", () => {
    expect(parseClientMessage({ type: "leave" })).toEqual({ type: "leave" });
    expect(parseClientMessage({ type: "nope" })).toBeNull();
    expect(parseClientMessage(null)).toBeNull();
    expect(parseClientMessage(42)).toBeNull();
  });
});

describe("frame codec", () => {
  it("decodeClientFrame returns null on bad JSON", () => {
    expect(decodeClientFrame("{not json")).toBeNull();
  });
  it("round-trips through encode + JSON.parse", () => {
    const msg: ServerMessage = { type: "welcome", peer: 1, peers: [2, 3] };
    expect(JSON.parse(encodeServerMessage(msg))).toEqual(msg);
  });

  it("replays canonical positive and rejection fixtures", () => {
    for (const entry of framesFixture.frames) {
      const decoded =
        entry.direction === "client"
          ? decodeClientFrame(JSON.stringify(entry.wire))
          : decodeServerFrame(JSON.stringify(entry.wire));
      expect(decoded, entry.label).toEqual(entry.wire);
    }

    for (const entry of framesFixture.rejects) {
      const decoded =
        entry.direction === "client"
          ? decodeClientFrame(JSON.stringify(entry.wire))
          : decodeServerFrame(JSON.stringify(entry.wire));
      expect(decoded, entry.label).toBeNull();
    }

    for (const entry of antiSpoofFixture.rejects) {
      expect(
        decodeClientFrame(JSON.stringify(entry.input.recv)),
        entry.label,
      ).toBeNull();
    }
  });
});
