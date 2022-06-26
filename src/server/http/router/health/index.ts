import { Express } from "express"

export default class HealthRouter {
  registerRoutes(expressApp: Express) {
    HealthRouter.registerHealthCheck(expressApp);
  }

  private static registerHealthCheck(expressApp: Express) {
    expressApp.get("/health", (_, response) => {
      // TODO: ensure DB connection is established?
      response.status(200).json({ status: "pass" });
    });
  }
}
