import net from "node:net";
import type { AddressInfo } from "node:net";

export type FakeClamMode = "ok" | "found" | "hang";

/**
 * Minimal ClamAV zINSTREAM server used to inject scanner failures against the
 * real `scanForMalware` client (no HTTP mocking, no client mocking):
 *
 * - "ok": reads the whole instream and replies `stream: OK`.
 * - "found": replies `stream: Eicar-Test-Signature FOUND` after the header.
 * - "hang": accepts the connection and never replies, exercising the client's
 *   socket timeout (scan timeout injection).
 */
export class FakeClamServer {
  private server: net.Server | undefined;
  private port = 0;
  readonly connections: net.Socket[] = [];

  start(mode: FakeClamMode): Promise<number> {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        this.connections.push(socket);
        socket.on("error", () => socket.destroy());

        if (mode === "found") {
          socket.write("stream: Eicar-Test-Signature FOUND\\0");
          socket.end();
          return;
        }
        if (mode === "hang") {
          // Never reply; the client-side timeout must fire.
          return;
        }

        // Drain the zINSTREAM command header, then the length-prefixed frames,
        // answering OK once the zero-length frame ends the stream (like clamd).
        let header = Buffer.alloc(0);
        let headerConsumed = false;
        let buffer = Buffer.alloc(0);
        let expected = -1;
        socket.on("data", (chunk) => {
          if (!headerConsumed) {
            header = Buffer.concat([header, chunk]);
            const nul = header.indexOf(0);
            if (nul === -1) return;
            headerConsumed = true;
            chunk = header.subarray(nul + 1);
            header = Buffer.alloc(0);
            if (chunk.length === 0) return;
          }
          buffer = Buffer.concat([buffer, chunk]);
          for (;;) {
            if (expected === -1) {
              if (buffer.length < 4) return;
              expected = buffer.readUInt32BE(0);
              buffer = buffer.subarray(4);
              if (expected === 0) {
                socket.write("stream: OK\\0");
                socket.end();
                return;
              }
            } else {
              if (buffer.length < expected) return;
              buffer = buffer.subarray(expected);
              expected = -1;
            }
          }
        });
      });
      this.server.on("error", reject);
      this.server.listen(0, "127.0.0.1", () => {
        this.port = (this.server!.address() as AddressInfo).port;
        resolve(this.port);
      });
    });
  }

  async stop(): Promise<void> {
    for (const socket of this.connections) socket.destroy();
    await new Promise<void>((resolve) => {
      if (!this.server) return resolve();
      const force = setTimeout(() => {
        for (const socket of this.connections) socket.destroy();
        this.server?.close(() => resolve());
      }, 1_000);
      force.unref();
      this.server.close(() => {
        clearTimeout(force);
        resolve();
      });
    });
    this.server = undefined;
  }
}
