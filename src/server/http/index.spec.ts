import { createHttpServer } from "./index"
import { getDurabilityEngine } from "../../engine"
import AsyncLock from "../../lock/asyncLock"
import DueueController from "../../controller/dueue"
import request from "supertest"
import { setTimeout } from "timers/promises"

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

  it("should expire messages", async () => {
    const message = "test";
    const expiresAfter = 3 * 1000;
    const publishResponse = await request(expressApp)
      .post("/dueue/expiry-test?acknowledgementDeadline=0")
      .set("Content-type", "application/json")
      .send({
        message,
        expiry: new Date().getTime() + expiresAfter
      });
    expect(publishResponse.status).toEqual(204);

    const receiveResponse = await request(expressApp).get(
      "/dueue/expiry-test"
    );
    expect(receiveResponse.status).toEqual(200);
    expect(receiveResponse.body.message).toEqual(message);

    const id = receiveResponse.body.id;
    expect(id).toBeDefined();

    await setTimeout(expiresAfter);

    const receiveAfterExpiryResponse = await request(expressApp).get(
      "/dueue/expiry-test"
    );
    expect(receiveAfterExpiryResponse.status).toEqual(404);
  });

  it("should restore unacknowledged messages", async () => {
    const message = "test";
    const publishResponse = await request(expressApp)
      .post("/dueue/unacknowledged-test")
      .set("Content-type", "application/json")
      .send({
        message,
      });
    expect(publishResponse.status).toEqual(204);

    const receiveResponse = await request(expressApp).get(
      "/dueue/unacknowledged-test?acknowledgementDeadline=0"
    );
    expect(receiveResponse.status).toEqual(200);
    expect(receiveResponse.body.message).toEqual(message);

    const id = receiveResponse.body.id;
    expect(id).toBeDefined();

    const receiveAfterUnacknowledgedResponse = await request(expressApp).get(
      "/dueue/unacknowledged-test"
    );
    expect(receiveAfterUnacknowledgedResponse.status).toEqual(200);
    const receiveAfterUnacknowledgedId = receiveAfterUnacknowledgedResponse.body.id;
    expect(receiveAfterUnacknowledgedId).toEqual(id);
  });

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
