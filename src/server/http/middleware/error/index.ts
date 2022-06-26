import { NextFunction, Request, Response } from "express"

export default function handleError(
  error: any,
  request: Request,
  response: Response,
  next: NextFunction
) {
  console.error(`${new Date()} An unexpected error occurred.`);
  console.error(error);
  response.status(500).send();
}
