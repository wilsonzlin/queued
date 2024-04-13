use percent_encoding::utf8_percent_encode;
use percent_encoding::NON_ALPHANUMERIC;
use reqwest::header::CONTENT_TYPE;
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DurationSeconds;
use std::error::Error;
use std::fmt::Display;
use std::time::Duration;

#[derive(Debug)]
pub enum QueuedClientError {
  Api {
    status: u16,
    error: String,
    error_details: Option<String>,
  },
  Unauthorized,
  Request(reqwest::Error),
}

impl Display for QueuedClientError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      QueuedClientError::Api {
        status,
        error,
        error_details,
      } => write!(f, "API error ({status} - {error}): {error_details:?}"),
      QueuedClientError::Unauthorized => write!(f, "unauthorized"),
      QueuedClientError::Request(e) => write!(f, "request error: {e}"),
    }
  }
}

impl Error for QueuedClientError {}

pub type QueuedClientResult<T> = Result<T, QueuedClientError>;

#[derive(Clone, Debug)]
pub struct QueuedClientCfg {
  pub api_key: Option<String>,
  pub endpoint: String,
}

#[derive(Clone, Debug)]
pub struct QueuedClient {
  r: reqwest::Client,
  cfg: QueuedClientCfg,
}

impl QueuedClient {
  pub fn with_request_client(request_client: reqwest::Client, cfg: QueuedClientCfg) -> Self {
    Self {
      r: request_client,
      cfg,
    }
  }

  pub fn new(cfg: QueuedClientCfg) -> Self {
    Self::with_request_client(reqwest::Client::new(), cfg)
  }

  async fn raw_request<I: Serialize, O: DeserializeOwned>(
    &self,
    method: Method,
    path: impl AsRef<str>,
    body: Option<&I>,
  ) -> QueuedClientResult<O> {
    let mut req = self
      .r
      .request(method, format!("{}{}", self.cfg.endpoint, path.as_ref()))
      .header("accept", "application/msgpack");
    if let Some(k) = &self.cfg.api_key {
      req = req.header("authorization", k);
    };
    if let Some(b) = body {
      let raw = rmp_serde::to_vec_named(b).unwrap();
      req = req.header("content-type", "application/msgpack").body(raw);
    };
    let res = req
      .send()
      .await
      .map_err(|err| QueuedClientError::Request(err))?;
    let status = res.status().as_u16();
    let res_type = res
      .headers()
      .get(CONTENT_TYPE)
      .and_then(|v| v.to_str().ok().map(|v| v.to_string()))
      .unwrap_or_default();
    let res_body_raw = res
      .bytes()
      .await
      .map_err(|err| QueuedClientError::Request(err))?;
    if status == 401 {
      return Err(QueuedClientError::Unauthorized);
    };
    #[derive(Deserialize)]
    struct ApiError {
      error: String,
      error_details: Option<String>,
    }
    if status < 200 || status > 299 || !res_type.starts_with("application/msgpack") {
      // The server may be behind some proxy, LB, etc., so we don't know what the body looks like for sure.
      return Err(match rmp_serde::from_slice::<ApiError>(&res_body_raw) {
        Ok(api_error) => QueuedClientError::Api {
          status,
          error: api_error.error,
          error_details: api_error.error_details,
        },
        Err(_) => QueuedClientError::Api {
          status,
          // We don't know if the response contains valid UTF-8 text or not.
          error: String::from_utf8_lossy(&res_body_raw).into_owned(),
          error_details: None,
        },
      });
    };
    Ok(rmp_serde::from_slice(&res_body_raw).unwrap())
  }

  pub fn queue(&self, queue_name: &str) -> QueuedQueueClient {
    QueuedQueueClient {
      c: self.clone(),
      qpp: format!(
        "/queue/{}",
        utf8_percent_encode(&queue_name, NON_ALPHANUMERIC)
      ),
    }
  }
}

#[derive(Clone, Debug)]
pub struct QueuedQueueClient {
  c: QueuedClient,
  qpp: String,
}

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Message {
  pub id: u64,
  pub poll_tag: u32,
}

impl From<PolledMessage> for Message {
  fn from(value: PolledMessage) -> Self {
    value.message()
  }
}

#[derive(Deserialize, Clone, Debug)]
pub struct PolledMessage {
  #[serde(with = "serde_bytes")]
  pub contents: Vec<u8>,
  pub id: u64,
  pub poll_tag: u32,
}

impl PolledMessage {
  pub fn message(&self) -> Message {
    Message {
      id: self.id,
      poll_tag: self.poll_tag,
    }
  }
}

#[derive(Deserialize)]
pub struct PollMessagesOutput {
  pub messages: Vec<PolledMessage>,
}

#[serde_as]
#[derive(Serialize)]
pub struct PushMessage {
  #[serde(with = "serde_bytes")]
  pub contents: Vec<u8>,
  #[serde_as(as = "DurationSeconds<u64>")]
  #[serde(rename = "visibility_timeout_secs")]
  pub visibility_timeout: Duration,
}

#[derive(Deserialize)]
pub struct PushMessagesOutput {
  pub ids: Vec<u64>,
}

#[derive(Deserialize)]
pub struct UpdateMessageOutput {
  pub new_poll_tag: u32,
}

#[derive(Deserialize)]
pub struct DeleteMessagesOutput {}

impl QueuedQueueClient {
  pub async fn poll_messages(
    &self,
    count: u64,
    visibility_timeout: Duration,
  ) -> QueuedClientResult<PollMessagesOutput> {
    #[derive(Serialize)]
    struct Input {
      count: u64,
      visibility_timeout_secs: u64,
    }
    self
      .c
      .raw_request(
        Method::POST,
        format!("{}/messages/poll", self.qpp),
        Some(&Input {
          count,
          visibility_timeout_secs: visibility_timeout.as_secs(),
        }),
      )
      .await
  }

  pub async fn push_messages(
    &self,
    msgs: impl AsRef<[PushMessage]>,
  ) -> QueuedClientResult<PushMessagesOutput> {
    #[derive(Serialize)]
    struct Input<'a> {
      messages: &'a [PushMessage],
    }
    self
      .c
      .raw_request(
        Method::POST,
        format!("{}/messages/push", self.qpp),
        Some(&Input {
          messages: msgs.as_ref(),
        }),
      )
      .await
  }

  pub async fn update_message(
    &self,
    m: Message,
    new_visibility_timeout: Duration,
  ) -> QueuedClientResult<UpdateMessageOutput> {
    #[derive(Serialize)]
    struct Input {
      id: u64,
      poll_tag: u32,
      visibility_timeout_secs: u64,
    }
    self
      .c
      .raw_request(
        Method::POST,
        format!("{}/messages/update", self.qpp),
        Some(&Input {
          id: m.id,
          poll_tag: m.poll_tag,
          visibility_timeout_secs: new_visibility_timeout.as_secs(),
        }),
      )
      .await
  }

  pub async fn delete_messages(
    &self,
    msgs: impl IntoIterator<Item = Message>,
  ) -> QueuedClientResult<DeleteMessagesOutput> {
    #[derive(Serialize)]
    struct Input {
      messages: Vec<Message>,
    }
    self
      .c
      .raw_request(
        Method::POST,
        format!("{}/messages/delete", self.qpp),
        Some(&Input {
          messages: msgs.into_iter().collect(),
        }),
      )
      .await
  }
}
