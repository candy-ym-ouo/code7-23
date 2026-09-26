import net from "node:net";
import { config } from "./config";

export async function scanForMalware(buffer: Buffer): Promise<void> {
  if (!config.CLAMAV_ENABLED) return;

  await new Promise<void>((resolve, reject) => {
    const socket = net.createConnection({ host: config.CLAMAV_HOST, port: config.CLAMAV_PORT });
    const chunks: Buffer[] = [];
    let settled = false;

    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      if (error) reject(error);
      else {
        const response = Buffer.concat(chunks).toString("utf8");
        if (response.includes("FOUND")) reject(new Error(`Malware detected: ${response.trim()}`));
        else if (!response.includes("OK")) reject(new Error(`ClamAV returned an unexpected response: ${response.trim()}`));
        else resolve();
      }
    };

    socket.setTimeout(config.CLAMAV_TIMEOUT_MS, () => finish(new Error(`ClamAV scan timed out after ${config.CLAMAV_TIMEOUT_MS}ms`)));
    socket.on("error", (error) => finish(error));
    socket.on("data", (chunk) => {
      chunks.push(chunk);
      if (Buffer.concat(chunks).includes(0)) finish();
    });
    socket.on("end", () => finish());
    socket.on("connect", () => {
      socket.write("zINSTREAM\0");
      const chunkSize = 64 * 1024;
      for (let offset = 0; offset < buffer.length; offset += chunkSize) {
        const chunk = buffer.subarray(offset, Math.min(offset + chunkSize, buffer.length));
        const size = Buffer.alloc(4);
        size.writeUInt32BE(chunk.length, 0);
        socket.write(size);
        socket.write(chunk);
      }
      socket.write(Buffer.alloc(4));
    });
  });
}
