import type { LogLevel } from "./log_levels";

export class DataDogTransport {
  readonly apiUrl = `https://http-intake.logs.datadoghq.com/api/v2/logs?dd-api-key=${process.env.DATADOG_API_KEY}`;

  constructor() {}

  async send(level: LogLevel, message: string, maybeContextOrError?: any) {
    const isError = maybeContextOrError instanceof Error;
    const postBody = {
      level,
      ddtags: `env:${process.env.NEXT_PUBLIC_DIGS_ENV},status:${level}`,
      message,
      service: "digs-gdc",
      ddsource: "hasura-weaviate-gdc",
    } as any;

    if (isError) {
      postBody.stack = maybeContextOrError.stack;
    } else if (maybeContextOrError) {
      postBody.meta = maybeContextOrError;
    }

    try {
      const response = await fetch(this.apiUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(postBody),
      });
      return await response.json();
    } catch (error: any) {
      console.error(error);
    }
  }

  async info(message: string, context?: object) {
    return this.send("info", message, context);
  }

  async debug(message: string, context?: object) {
    return this.send("debug", message, context);
  }

  async warn(message: string, context?: object) {
    return this.send("warn", message, context);
  }

  /**
   * Rather than check instanceof Error in every catch block,
   * we will take `any` here, meaning that we need to check if the argument is an Error
   * and pass it to the correct argument for DataDog.
   */
  async error(message: string, maybeContextOrError?: any) {
    return this.send("error", message, maybeContextOrError);
  }
}
