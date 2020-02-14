FROM tritonmedia/base

COPY --chown=999:999 package.json yarn.lock /stack/
RUN yarn --production=true --frozen-lockfile

COPY --chown=999:999 . /stack
