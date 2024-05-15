import { inspect } from "util";
import { bold, grey } from "ansis";

/**
 * when colorizing the message, it gets cast as a string. This will result in loss of information on certain data types.
 */
export const boldIfSafe = (message: any): string => {
  switch (typeof message) {
    case "string":
      return bold(message);
    case "boolean":
    case "number":
      return bold(message.toString());
    default:
      return bold(inspect(message, { colors: false, depth: null }));
  }
};

/**
 * get a grayed out printout of the file path and line number of a log message.
 */
export const getFileAndLineNumber = () => {
  if (process.env.INCLUDE_TRACE_IN_LOGS !== "true") return "";
  let initiator = "(unknown place)";
  try {
    // throwing and catching an error seems to be the best way to get the stack trace
    throw new Error();
  } catch (e: Error | any) {
    const stack = e?.stack;
    if (typeof stack === "string") {
      let index: number | undefined;
      const lines = stack.split("\n");

      for (let i = 0; i < lines.length; i++) {
        // find the first line of the actual stack trace
        if (lines[i].match(/^\s+at\s+(.*)/)) {
          index = i;
          break;
        }
      }
      if (index && lines.length > index + 2) {
        // use the line 2 after the initial line since the first two lines are:
        // 1. this function
        // 2. the log function
        const correctLine = lines[index + 2].match(/^\s+at\s+(.*)/)?.[1];
        if (!correctLine) return grey(initiator);
        initiator = correctLine;
        // remove this stuff since i don't think we need it
        const components = initiator.split("webpack-internal:///(api)/./");
        if (components.length > 1) {
          initiator = `(${components[1]}`;
        } else {
          // if for whatever reason the output doesn't have that webpack stuff then still clean it up a little
          initiator = `(${initiator.substring(initiator.indexOf("(") + 1)}`;
        }
      }
    }
    return grey(initiator);
  }
};
