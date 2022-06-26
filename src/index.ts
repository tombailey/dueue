import { createHttpServer } from "./server/http"
import { getDurabilityEngine } from "./engine"
import DueueController from "./controller/dueue"
import Lock from "./lock"
import AsyncLock from "./lock/asyncLock"

async function init() {
  const durabilityEngine = await getDurabilityEngine();
  const lock: Lock = new AsyncLock();

  const httpPort = process.env["HTTP_PORT"];

  if (httpPort === undefined) {
    throw new Error("'HTTP_PORT' should be specified.");
  }

  const durabilityController = new DueueController(
    await durabilityEngine.getQueues(),
    durabilityEngine,
    lock
  );
  if (httpPort) {
    const httpServer = await createHttpServer(durabilityController);
    httpServer.listen(httpPort, () => {
      console.log("Listening for HTTP API requests.");
    });
  }
}

init();
