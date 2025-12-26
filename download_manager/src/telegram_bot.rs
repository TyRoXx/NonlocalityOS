use std::sync::Arc;
use teloxide::{
    dispatching::UpdateFilterExt,
    dptree,
    prelude::{Dispatcher, Requester},
    types::{Message, Update},
    Bot,
};
use tracing::{info, warn};

#[async_trait::async_trait]
pub trait HandleTelegramBotRequests {
    async fn add_download_job(&self, url: &str) -> Option<String>;
}

#[async_trait::async_trait]
pub trait TelegramBot {
    async fn run(&self, handle_requests: Arc<dyn HandleTelegramBotRequests + Send + Sync>);
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessMessageResultingAction {
    SendMessage(String),
}

pub async fn process_message_impl(
    message: &str,
    handle_requests: &(dyn HandleTelegramBotRequests + Send + Sync),
) -> Result<ProcessMessageResultingAction, Box<dyn std::error::Error + Send + Sync>> {
    let error = handle_requests.add_download_job(message).await;
    let response = match &error {
        Some(message) => format!("Failed to add download job: {}", message),
        None => "Successfully added download job".to_string(),
    };
    Ok(ProcessMessageResultingAction::SendMessage(response))
}

pub async fn process_message(
    bot: Bot,
    message: Message,
    allowed_user: &teloxide::types::UserId,
    handle_requests: &(dyn HandleTelegramBotRequests + Send + Sync),
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match &message.from {
        Some(user) => {
            info!("Received message from user {:?}: {:?}", &user, &message);
            if user.id != *allowed_user {
                warn!(
                    "User {:?} (ID: {}) is not allowed to use this bot, ignoring",
                    &user.username, user.id
                );
                return Ok(());
            }
            match message.text() {
                Some(text) => {
                    let action = process_message_impl(text, handle_requests).await?;
                    match action {
                        ProcessMessageResultingAction::SendMessage(response) => {
                            bot.send_message(message.chat.id, response).await?;
                        }
                    }
                }
                None => {
                    warn!("Received message without text, ignoring");
                }
            }
        }
        None => {
            warn!("Received message from unknown user, ignoring");
        }
    }
    Ok(())
}

pub struct TeloxideTelegramBot {
    pub telegram_api_token: String,
    pub allowed_user: teloxide::types::UserId,
}

struct SharedState {
    pub allowed_user: teloxide::types::UserId,
    pub handle_requests: Arc<dyn HandleTelegramBotRequests + Send + Sync>,
}

#[async_trait::async_trait]
impl TelegramBot for TeloxideTelegramBot {
    async fn run(&self, handle_requests: Arc<dyn HandleTelegramBotRequests + Send + Sync>) {
        info!("Starting Telegram bot...");
        let bot = Bot::new(&self.telegram_api_token);
        let state = Arc::new(SharedState {
            allowed_user: self.allowed_user,
            handle_requests,
        });
        let handler = Update::filter_message().endpoint(
            |bot: Bot, state: Arc<SharedState>, msg: Message| async move {
                let handle_requests = state.handle_requests.clone();
                process_message(bot, msg, &state.allowed_user, handle_requests.as_ref()).await
            },
        );
        Dispatcher::builder(bot, handler)
            .dependencies(dptree::deps![state])
            .build()
            .dispatch()
            .await;
        info!("Telegram bot stopped.");
    }
}
