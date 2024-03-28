import { decode, encode } from "@msgpack/msgpack";
import { VArray, VBytes, VInteger, VString, VStruct } from "@wzlin/valid";
import mapExists from "@xtjs/lib/js/mapExists";
import withoutUndefined from "@xtjs/lib/js/withoutUndefined";

export class QueuedError extends Error {
  constructor(
    readonly status: number,
    readonly error: string,
    readonly errorDetails: any | undefined,
  ) {
    super(`Request to Queued failed with status ${status}: ${error}`);
  }
}

// Queue path prefix.
const qpp = (q: string) => `/queue/${encodeURIComponent(q)}`;

export class QueuedClient {
  constructor(
    private readonly opts: {
      apiKey: string;
      endpoint: string;
    },
  ) {}

  private async req(method: string, path: string, body: any) {
    const res = await fetch(`${this.opts.endpoint}${path}`, {
      method,
      headers: withoutUndefined({
        Authorization: this.opts.apiKey,
        "Content-Type": mapExists(body, () => "application/msgpack"),
      }),
      body: mapExists(body, encode),
    });
    const resBodyRaw = new Uint8Array(await res.arrayBuffer());
    const resBody: any = decode(resBodyRaw);
    if (!res.ok) {
      throw new QueuedError(
        res.status,
        resBody?.error,
        resBody?.errorDetails ?? undefined,
      );
    }
    return resBody;
  }

  async deleteQueue(q: string) {
    await this.req("DELETE", qpp(q), undefined);
  }

  async createQueue(q: string) {
    await this.req("PUT", qpp(q), undefined);
  }

  async deleteQueueMessages(
    q: string,
    messages: Array<{ id: number; pollTag: number }>,
  ) {
    await this.req("POST", `${qpp(q)}/messages/delete`, {
      // Don't just provide `messages` as it may have other properties.
      messages: messages.map((m) => ({
        id: m.id,
        poll_tag: m.pollTag,
      })),
    });
  }

  async pollQueueMessages(
    q: string,
    count: number,
    visibilityTimeoutSecs: number,
  ) {
    const raw = await this.req("POST", `${qpp(q)}/messages/poll`, {
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

  async pushQueueMessages(
    q: string,
    messages: Array<{
      contents: Uint8Array;
      visibilityTimeoutSecs: number;
    }>,
  ) {
    // Don't just provide `messages` as it may have other properties.
    const raw = await this.req("POST", `${qpp(q)}/messages/push`, {
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

  async updateQueueMessage(
    q: string,
    message: {
      id: number;
      pollTag: number;
      visibilityTimeoutSecs: number;
    },
  ) {
    // Don't just provide `message` as it may have other properties.
    const raw = await this.req("POST", `${qpp(q)}/messages/update`, {
      id: message.id,
      poll_tag: message.pollTag,
      visibility_timeout_secs: message.visibilityTimeoutSecs,
    });
    const p = new VStruct({
      new_poll_tag: new VInteger(0),
    }).parseRoot(raw);
    return p.new_poll_tag;
  }

  async listQueues() {
    const raw = await this.req("GET", "/queues", undefined);
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
