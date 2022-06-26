import { createHttpServer } from "./index"
import { getDurabilityEngine } from "../../engine"
import AsyncLock from "../../lock/asyncLock"
import DueueController from "../../controller/dueue"
import request from "supertest"

describe("Test dueue HTTP API", () => {
  let expressApp: Express.Application;

  beforeAll(async () => {
    expressApp = await createExpressApp();
  });

  it("should allow messages to be published", async () => {
    const publishResponse = await request(expressApp)
      .post("/dueue/publish-test")
      .set("Content-type", "application/json")
      .send({
        message: "test",
      });
    expect(publishResponse.status).toEqual(204);
  });

  it("should allow messages to be received", async () => {
    const message = "test";
    const publishResponse = await request(expressApp)
      .post("/dueue/receive-test")
      .set("Content-type", "application/json")
      .send({
        message,
      });
    expect(publishResponse.status).toEqual(204);

    const receiveResponse = await request(expressApp).get(
      "/dueue/receive-test"
    );
    expect(receiveResponse.status).toEqual(200);
    expect(receiveResponse.body.message).toEqual(message);
    expect(receiveResponse.body.id).toBeDefined();
  });

  it("should allow messages to be acknowledged", async () => {
    const message = "test";
    const publishResponse = await request(expressApp)
      .post("/dueue/acknowledge-test")
      .set("Content-type", "application/json")
      .send({
        message,
      });
    expect(publishResponse.status).toEqual(204);

    const receiveResponse = await request(expressApp).get(
      "/dueue/acknowledge-test"
    );
    expect(receiveResponse.status).toEqual(200);
    expect(receiveResponse.body.message).toEqual(message);

    const id = receiveResponse.body.id;
    expect(id).toBeDefined();

    const acknowledgeResponse = await request(expressApp)
      .delete(`/dueue/acknowledge-test/${id}`)
      .send();
    expect(acknowledgeResponse.status).toEqual(204);

    const receiveAfterAcknowledgeResponse = await request(expressApp).get(
      "/dueue/acknowledge-test"
    );
    expect(receiveAfterAcknowledgeResponse.status).toEqual(404);
  });

  // TODO: test expiry and acknowledgement deadline

  async function createExpressApp() {
    const durabilityEngine = await getDurabilityEngine("memory");
    return createHttpServer(
      new DueueController(
        await durabilityEngine.getQueues(),
        durabilityEngine,
        new AsyncLock()
      )
    );
  }
});
