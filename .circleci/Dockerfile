FROM jaredallard/triton-base

COPY --chown=999:999 package.json yarn.lock /stack/
RUN yarn --production=true

COPY --chown=999:999 . /stack
