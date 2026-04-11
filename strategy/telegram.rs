use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct TelegramClient {
    base_url: String,
    http: Client,
}

#[derive(Debug, Clone)]
pub struct TelegramUpdate {
    pub update_id: i64,
    pub chat_id: i64,
    pub text: String,
}

#[derive(Debug, Deserialize)]
struct GetUpdatesResponse {
    ok: bool,
    result: Vec<UpdateItem>,
}

#[derive(Debug, Deserialize)]
struct UpdateItem {
    update_id: i64,
    message: Option<MessageItem>,
}

#[derive(Debug, Deserialize)]
struct MessageItem {
    chat: ChatItem,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatItem {
    id: i64,
}

impl TelegramClient {
    pub fn new(bot_token: &str) -> Self {
        Self {
            base_url: format!("https://api.telegram.org/bot{}", bot_token),
            http: Client::new(),
        }
    }

    pub async fn get_updates(&self, offset: i64) -> Result<Vec<TelegramUpdate>, reqwest::Error> {
        let url = format!("{}/getUpdates", self.base_url);
        let response = self
            .http
            .get(url)
            .query(&[("timeout", "20"), ("offset", &offset.to_string())])
            .send()
            .await?
            .json::<GetUpdatesResponse>()
            .await?;

        if !response.ok {
            return Ok(Vec::new());
        }

        let updates = response
            .result
            .into_iter()
            .filter_map(|item| {
                let message = item.message?;
                let text = message.text?;
                Some(TelegramUpdate {
                    update_id: item.update_id,
                    chat_id: message.chat.id,
                    text,
                })
            })
            .collect();

        Ok(updates)
    }

    pub async fn send_message(&self, chat_id: i64, text: &str) -> Result<(), reqwest::Error> {
        let url = format!("{}/sendMessage", self.base_url);
        self.http
            .post(url)
            .form(&[("chat_id", chat_id.to_string()), ("text", text.to_owned())])
            .send()
            .await?;
        Ok(())
    }
}
