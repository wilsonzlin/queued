import { decode, encode } from "@msgpack/msgpack";
import { VArray, VBytes, VInteger, VString, VStruct } from "@wzlin/valid";
import encodeBase64 from "@xtjs/lib/js/encodeBase64";
import mapExists from "@xtjs/lib/js/mapExists";
import withoutUndefined from "@xtjs/lib/js/withoutUndefined";

export class QueuedUnauthorizedError extends Error {
  constructor() {
    super("Authorization failed");
  }
}

export class QueuedApiError extends Error {
  constructor(
    readonly status: number,
    readonly error: string,
    readonly errorDetails: any | undefined,
  ) {
    super(
      `Request to Queued failed with status ${status}: ${error} ${JSON.stringify(errorDetails, null, 2) ?? ""}`,
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

  async pollMessagesRaw(count: number, visibilityTimeoutSecs: number) {
    const raw = await this.svc.rawRequest("POST", `${this.qpp}/messages/poll`, {
      count,
      visibility_timeout_secs: visibilityTimeoutSecs,
    });
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
  ) {
    const res = await this.pollMessagesRaw(count, visibilityTimeoutSecs);
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
        visibility_timeout_secs: m.visibilityTimeoutSecs,
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
        contents: encode(m),
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
        visibility_timeout_secs: newVisibilityTimeoutSecs,
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
      apiKey: string;
      endpoint: string;
    },
  ) {}

  queue(queueName: string) {
    return new QueuedQueueClient(this, queueName);
  }

  async rawRequest(method: string, path: string, body: any) {
    const res = await fetch(`${this.opts.endpoint}${path}`, {
      method,
      headers: withoutUndefined({
        Authorization: this.opts.apiKey,
        "Content-Type": mapExists(body, () => "application/msgpack"),
      }),
      body: mapExists(body, encode),
    });
    const resBodyRaw = new Uint8Array(await res.arrayBuffer());
    if (res.status === 401) {
      throw new QueuedUnauthorizedError();
    }
    let resBody: any;
    try {
      resBody = decode(resBodyRaw);
    } catch (err) {
      throw new Error(
        `Failed to decode MessagePack response: ${encodeBase64(resBodyRaw)}`,
        { cause: err },
      );
    }
    if (!res.ok) {
      throw new QueuedApiError(
        res.status,
        resBody?.error,
        resBody?.error_details ?? undefined,
      );
    }
    return resBody;
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
