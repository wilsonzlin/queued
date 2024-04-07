import { decode, encode } from "@msgpack/msgpack";
import { VArray, VBytes, VInteger, VString, VStruct } from "@wzlin/valid";
import asyncTimeout from "@xtjs/lib/js/asyncTimeout";
import bufferToUint8Array from "@xtjs/lib/js/bufferToUint8Array";
import decodeUtf8 from "@xtjs/lib/js/decodeUtf8";
import mapExists from "@xtjs/lib/js/mapExists";
import withoutUndefined from "@xtjs/lib/js/withoutUndefined";
import http, { IncomingMessage } from "node:http";
import https from "node:https";

export class QueuedUnauthorizedError extends Error {
  constructor() {
    super("Authorization failed");
  }
}

export class QueuedApiError extends Error {
  constructor(
    readonly status: number,
    readonly error: string | undefined,
    readonly errorDetails: any | undefined,
  ) {
    super(
      `Request to queued failed with status ${status}: ${error} ${JSON.stringify(errorDetails, null, 2) ?? ""}`,
    );
  }
}

// Queue path prefix.
const qpp = (name: string) => `/queue/${encodeURIComponent(name)}`;

export type MsgPackValue =
  | null
  | undefined
  | boolean
  | number
  | string
  | Date
  | ArrayBufferView
  | ReadonlyArray<MsgPackValue>
  | {
      readonly [k: string | number]: MsgPackValue;
    };

export class QueuedQueueClient {
  constructor(
    private readonly svc: QueuedClient,
    private readonly queue: string,
  ) {}

  private get qpp() {
    return qpp(this.queue);
  }

  async metrics() {
    const raw = await this.svc.rawRequest(
      "GET",
      `${this.qpp}/metrics`,
      undefined,
    );
    const p = new VStruct({
      empty_poll_counter: new VInteger(0),
      message_counter: new VInteger(0),
      missing_delete_counter: new VInteger(0),
      missing_update_counter: new VInteger(0),
      successful_delete_counter: new VInteger(0),
      successful_poll_counter: new VInteger(0),
      successful_push_counter: new VInteger(0),
      successful_update_counter: new VInteger(0),
      suspended_delete_counter: new VInteger(0),
      suspended_poll_counter: new VInteger(0),
      suspended_push_counter: new VInteger(0),
      suspended_update_counter: new VInteger(0),
      throttled_poll_counter: new VInteger(0),

      first_message_visibility_timeout_sec_gauge: new VInteger(0),
      last_message_visibility_timeout_sec_gauge: new VInteger(0),
      longest_unpolled_message_sec_gauge: new VInteger(0),
    }).parseRoot(raw);
    return {
      emptyPollCounter: p.empty_poll_counter,
      messageCounter: p.message_counter,
      missingDeleteCounter: p.missing_delete_counter,
      missingUpdateCounter: p.missing_update_counter,
      successfulDeleteCounter: p.successful_delete_counter,
      successfulPollCounter: p.successful_poll_counter,
      successfulPushCounter: p.successful_push_counter,
      successfulUpdateCounter: p.successful_update_counter,
      suspendedDeleteCounter: p.suspended_delete_counter,
      suspendedPollCounter: p.suspended_poll_counter,
      suspendedPushCounter: p.suspended_push_counter,
      suspendedUpdateCounter: p.suspended_update_counter,
      throttledPollCounter: p.throttled_poll_counter,
      firstMessageVisibilityTimeoutSecGauge:
        p.first_message_visibility_timeout_sec_gauge,
      lastMessageVisibilityTimeoutSecGauge:
        p.last_message_visibility_timeout_sec_gauge,
      longestUnpolledMessageSecGauge: p.longest_unpolled_message_sec_gauge,
    };
  }

  async pollMessagesRaw(
    count: number,
    visibilityTimeoutSecs: number,
    ignoreExistingVisibilityTimeouts?: boolean,
  ) {
    const raw = await this.svc.rawRequest(
      "POST",
      `${this.qpp}/messages/poll`,
      withoutUndefined({
        count,
        visibility_timeout_secs: Math.floor(visibilityTimeoutSecs),
        ignore_existing_visibility_timeouts: ignoreExistingVisibilityTimeouts,
      }),
    );
    const p = new VStruct({
      messages: new VArray(
        new VStruct({
          contents: new VBytes(),
          id: new VInteger(0),
          poll_tag: new VInteger(0),
        }),
      ),
    }).parseRoot(raw);
    return p.messages.map((m) => ({
      contents: m.contents,
      id: m.id,
      pollTag: m.poll_tag,
    }));
  }

  async pollMessages<T extends MsgPackValue>(
    count: number,
    visibilityTimeoutSecs: number,
    ignoreExistingVisibilityTimeouts?: boolean,
  ) {
    const res = await this.pollMessagesRaw(
      count,
      visibilityTimeoutSecs,
      ignoreExistingVisibilityTimeouts,
    );
    return res.map(({ contents, ...r }) => ({
      ...r,
      contents: decode(contents) as T,
    }));
  }

  async pushMessagesRaw(
    messages: Array<{
      contents: Uint8Array;
      visibilityTimeoutSecs: number;
    }>,
  ) {
    // Don't just provide `messages` as it may have other properties.
    const raw = await this.svc.rawRequest("POST", `${this.qpp}/messages/push`, {
      messages: messages.map((m) => ({
        contents: m.contents,
        visibility_timeout_secs: Math.floor(m.visibilityTimeoutSecs),
      })),
    });
    const p = new VStruct({
      ids: new VArray(new VInteger(0)),
    }).parseRoot(raw);
    return p.ids;
  }

  async pushMessages(
    messages: Array<{
      contents: MsgPackValue;
      visibilityTimeoutSecs: number;
    }>,
  ) {
    return await this.pushMessagesRaw(
      messages.map(({ contents, ...m }) => ({
        ...m,
        contents: encode(contents),
      })),
    );
  }

  async updateMessage(
    message: {
      id: number;
      pollTag: number;
    },
    newVisibilityTimeoutSecs: number,
  ) {
    // Don't just provide `message` as it may have other properties.
    const raw = await this.svc.rawRequest(
      "POST",
      `${this.qpp}/messages/update`,
      {
        id: message.id,
        poll_tag: message.pollTag,
        visibility_timeout_secs: Math.floor(newVisibilityTimeoutSecs),
      },
    );
    const p = new VStruct({
      new_poll_tag: new VInteger(0),
    }).parseRoot(raw);
    return p.new_poll_tag;
  }

  async deleteMessages(messages: Array<{ id: number; pollTag: number }>) {
    await this.svc.rawRequest("POST", `${this.qpp}/messages/delete`, {
      // Don't just provide `messages` as it may have other properties.
      messages: messages.map((m) => ({
        id: m.id,
        poll_tag: m.pollTag,
      })),
    });
  }
}

export class QueuedClient {
  constructor(
    private readonly opts: {
      apiKey?: string;
      endpoint: string;
      // WARNING: Most operations mutate some state on the queue (e.g. push, poll).
      maxRetries?: number;
      ssl?: {
        key?: string;
        cert?: string;
        ca?: string;
        servername?: string;
        rejectUnauthorized?: boolean;
      };
    },
  ) {}

  queue(queueName: string) {
    return new QueuedQueueClient(this, queueName);
  }

  async rawRequest(method: string, path: string, body: any) {
    // Construct a URL to ensure it is correct. If it throws, we don't want to retry.
    const reqUrl = new URL(`${this.opts.endpoint}${path}`);
    const reqOpt: https.RequestOptions = {
      method,
      headers: withoutUndefined({
        Accept: "application/msgpack",
        Authorization: this.opts.apiKey,
        "Content-Type": mapExists(body, () => "application/msgpack"),
      }),
      ca: this.opts.ssl?.ca,
      cert: this.opts.ssl?.cert,
      key: this.opts.ssl?.key,
      servername: this.opts.ssl?.servername,
      rejectUnauthorized: this.opts.ssl?.rejectUnauthorized,
    };
    const reqBody = mapExists(body, encode);
    const { maxRetries = 1 } = this.opts;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const res = await new Promise<IncomingMessage>((resolve, reject) => {
          // For safety, assume https unless explicitly http.
          const req =
            reqUrl.protocol === "http:"
              ? http.request(reqUrl, reqOpt)
              : https.request(reqUrl, reqOpt);
          req.on("error", reject).on("response", resolve);
          req.end(reqBody);
        });
        const resBodyRaw = await new Promise<Buffer>((resolve, reject) => {
          const chunks = Array<Buffer>();
          res
            .on("error", reject)
            .on("data", (c) => chunks.push(c))
            .on("end", () => resolve(Buffer.concat(chunks)));
        });
        if (res.statusCode === 401) {
          throw new QueuedUnauthorizedError();
        }
        const resType = res.headers["content-type"] ?? "";
        const resBody: any = /^application\/(x-)?msgpack$/.test(resType)
          ? // It appears that if Buffer is passed to msgpack.decode, it will parse all bytes as Buffer, but if not, it will use Uint8Array. We want Uint8Array values for all bytes.
            decode(bufferToUint8Array(resBodyRaw))
          : decodeUtf8(resBodyRaw);
        if (res.statusCode! < 200 || res.statusCode! > 299) {
          throw new QueuedApiError(
            res.statusCode!,
            resBody?.error ?? resBody,
            resBody?.error_details ?? undefined,
          );
        }
        return resBody;
      } catch (err) {
        if (
          attempt === maxRetries ||
          err instanceof QueuedUnauthorizedError ||
          (err instanceof QueuedApiError && err.status < 500)
        ) {
          throw err;
        }
        await asyncTimeout(
          Math.random() * Math.min(1000 * 60 * 10, 2 ** attempt),
        );
      }
    }
  }

  async listApiKeys() {
    const res = await this.rawRequest("GET", "/api-keys", undefined);
    return new VStruct({
      keys: new VArray(
        new VStruct({
          key: new VString(),
          prefix: new VString(),
        }),
      ),
    }).parseRoot(res);
  }

  async deleteApiKey(apiKey: string) {
    await this.rawRequest(
      "DELETE",
      `/api-key/${encodeURIComponent(apiKey)}`,
      undefined,
    );
  }

  async setApiKey(apiKey: string, queueNamePrefix: string) {
    await this.rawRequest("PUT", `/api-key/${encodeURIComponent(apiKey)}`, {
      prefix: queueNamePrefix,
    });
  }

  async deleteQueue(q: string) {
    await this.rawRequest("DELETE", qpp(q), undefined);
  }

  async createQueue(q: string) {
    await this.rawRequest("PUT", qpp(q), undefined);
  }

  async listQueues() {
    const raw = await this.rawRequest("GET", "/queues", undefined);
    const p = new VStruct({
      queues: new VArray(
        new VStruct({
          name: new VString(),
        }),
      ),
    }).parseRoot(raw);
    return p;
  }
}
