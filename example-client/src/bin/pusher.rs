use serde_json::json;
use serde_json::Value;

#[tokio::main]
async fn main() {
  let client = reqwest::Client::new();
  let res = client
    .post("http://127.0.0.1:3333/push")
    .json(&json!({
      "content": "Hello, world!",
    }))
    .send()
    .await
    .unwrap()
    .error_for_status()
    .unwrap()
    .json::<Value>()
    .await
    .unwrap();
  println!("{}", res);
}
