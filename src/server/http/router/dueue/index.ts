import { Express } from "express"
import DueueController from "../../../../controller/dueue"

export default class DueueRouter {
  private readonly dueueController: DueueController;

  constructor(dueueController: DueueController) {
    this.dueueController = dueueController;
  }

  registerRoutes(expressApp: Express) {
    this.registerPublish(expressApp);
    this.registerReceive(expressApp);
    this.registerAcknowledge(expressApp);
  }

  private registerPublish(expressApp: Express) {
    expressApp.post("/dueue/:queueName", async (request, response) => {
      const queueName = request.params.queueName;
      const message = request.body.message;
      const expiry = request.body.expiry;
      if (typeof message !== "string") {
        response.status(400).json({
          message: "message is required.",
        });
      } else if (expiry !== undefined && typeof expiry !== "number") {
        response.status(400).json({
          message: "expiry should be omitted or a unix timestamp.",
        });
      } else {
        await this.dueueController.publishOne(queueName, {
          body: message,
          expiry:
            typeof expiry === "number" ? new Date(expiry * 1000) : undefined,
        });
        response.status(204).send();
      }
    });
  }

  private registerReceive(expressApp: Express) {
    expressApp.get("/dueue/:queueName", async (request, response) => {
      const queueName = request.params.queueName;
      const acknowledgementDeadline = request.query.acknowledgementDeadline;
      if (
        acknowledgementDeadline !== undefined &&
        (
          typeof acknowledgementDeadline !== "string" ||
          !/^\d+$/.test(acknowledgementDeadline)
        )
      ) {
        response.status(400).json({
          message:
            "acknowledgementDeadline should be omitted or a unix timestamp.",
        });
      }

      const message = await this.dueueController.receiveOne(
        queueName,
        typeof acknowledgementDeadline === "string"
          ? new Date(Number(acknowledgementDeadline) * 1000)
          : undefined
      );
      if (message === null) {
        response.status(404).send();
      } else {
        response.status(200).json({
          id: message.id,
          message: message.body,
        });
      }
    });
  }

  private registerAcknowledge(expressApp: Express) {
    expressApp.delete(
      "/dueue/:queueName/:messageId",
      async (request, response) => {
        const queueName = request.params.queueName;
        const messageId = request.params.messageId;

        await this.dueueController.acknowledgeOne(queueName, messageId);
        response.status(204).send();
      }
    );
  }
}
