import { format } from "date-fns";
import { cyan, green, red, yellow, hex } from "ansis";
import { boldIfSafe, getFileAndLineNumber } from "./utils";
import { DataDogTransport } from "./datadog_transport";

const isConsoleEnabled =
  process.env.NODE_ENV === "development" || process.env.NODE_ENV === "test";

function isDataDogEnabled(): boolean {
  return (
    !!process.env.DATADOG_API_KEY &&
    !!process.env.NEXT_PUBLIC_DIGS_ENV &&
    !!(process.env.DATADOG_SERVER_LOGGER_ENABLED === "true")
  );
}

class Logger {
  readonly dataDogTransport = new DataDogTransport();

  constructor() {}

  getConsoleArgs(maybeContextOrError?: any) {
    const isError = maybeContextOrError instanceof Error;
    const loggable = isError
      ? maybeContextOrError.stack
      : JSON.stringify(maybeContextOrError, null, 2);
    return maybeContextOrError
      ? [loggable, getFileAndLineNumber()]
      : [getFileAndLineNumber()];
  }

  getConsoleTimestamp() {
    return hex("#77916c")`[${format(new Date(), "HH:MM:ss")}]`;
  }

  async info(message: string, context?: object) {
    if (isConsoleEnabled) {
      console.info(
        this.getConsoleTimestamp(),
        cyan`→️  INFO`,
        boldIfSafe(message),
        ...this.getConsoleArgs(context)
      );
    }
    if (isDataDogEnabled()) {
      return this.dataDogTransport.info(message, context);
    }
  }

  async debug(message: string, context?: object) {
    if (isConsoleEnabled) {
      console.debug(
        this.getConsoleTimestamp(),
        green`🐛 DEBUG`,
        boldIfSafe(message),
        ...this.getConsoleArgs(context)
      );
    }
    if (isDataDogEnabled()) {
      return this.dataDogTransport.debug(message, context);
    }
  }

  async warn(message: string, context?: object) {
    if (isConsoleEnabled) {
      console.warn(
        this.getConsoleTimestamp(),
        yellow`⚠️  WARN`,
        boldIfSafe(message),
        ...this.getConsoleArgs(context)
      );
    }
    if (isDataDogEnabled()) {
      return this.dataDogTransport.warn(message, context);
    }
  }

  /**
   * Rather than check instanceof Error in every catch block,
   * we will take `any` here, meaning that we need to check if the argument is an Error
   * and pass it to the correct argument for DataDog.
   */
  async error(message: string, maybeContextOrError?: any) {
    if (isConsoleEnabled) {
      console.error(
        this.getConsoleTimestamp(),
        red.bold`‼️  ERROR`,
        boldIfSafe(message),
        ...this.getConsoleArgs(maybeContextOrError)
      );
    }
    if (isDataDogEnabled()) {
      return this.dataDogTransport.error(message, maybeContextOrError);
    }
  }
}

// namespaced differently to prevent accidentally auto-importing frontend logger
export const log = new Logger();
