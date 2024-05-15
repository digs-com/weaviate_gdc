FROM node:16-alpine

ARG NODE_ENV=development
ARG DATADOG_API_KEY
ARG NEXT_PUBLIC_DIGS_ENV=local
ARG DATADOG_SERVER_LOGGER_ENABLED=false
ARG INCLUDE_TRACE_IN_LOGS=false

ENV NODE_ENV $NODE_ENV
ENV DATADOG_API_KEY $DATADOG_API_KEY
ENV NEXT_PUBLIC_DIGS_ENV $NEXT_PUBLIC_DIGS_ENV
ENV DATADOG_SERVER_LOGGER_ENABLED $DATADOG_SERVER_LOGGER_ENABLED
ENV INCLUDE_TRACE_IN_LOGS $INCLUDE_TRACE_IN_LOGS

WORKDIR /app
COPY package.json .
COPY package-lock.json .

RUN npm ci

COPY tsconfig.json .
COPY src src

# This is just to ensure everything compiles ahead of time.
# We'll actually run using ts-node to ensure we get TypesScript
# stack traces if something fails at runtime.
RUN npm run typecheck

EXPOSE 8100

# We don't bother doing typechecking when we run (only TS->JS transpiling)
# because we checked it above already. This uses less memory at runtime.
CMD [ "npm", "run", "--silent", "start-no-typecheck" ]
