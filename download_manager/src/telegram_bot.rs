use std::sync::Arc;
use teloxide::{
    dispatching::UpdateFilterExt,
    dptree,
    payloads::SendMessageSetters,
    prelude::{Dispatcher, Requester},
    types::{Message, Update},
    Bot,
};
use tracing::{info, warn};

#[async_trait::async_trait]
pub trait HandleTelegramBotRequests {
    async fn add_download_job(&self, url: &str) -> Option<String>;
    async fn list_failed_downloads(&self) -> Vec<String>;
    async fn retry_failed_downloads(&self) -> (usize, usize);
}

#[async_trait::async_trait]
pub trait TelegramBot {
    async fn run(&self, handle_requests: Arc<dyn HandleTelegramBotRequests + Send + Sync>);
}

pub fn split_message_into_urls(message: &str) -> Vec<&str> {
    message
        .lines()
        .flat_map(|line| line.split_whitespace())
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessMessageResultingAction {
    SendMessage(String),
}

pub async fn process_message_impl(
    message: &str,
    handle_requests: &(dyn HandleTelegramBotRequests + Send + Sync),
) -> Result<ProcessMessageResultingAction, Box<dyn std::error::Error + Send + Sync>> {
    let mut urls = split_message_into_urls(message);
    // Queue the oldest videos first.
    urls.sort();
    let mut response = String::new();
    let mut success_count = 0;
    let mut failure_count = 0;
    for url in urls {
        let error = handle_requests.add_download_job(url).await;
        match &error {
            Some(message) => {
                failure_count += 1;
                response.push_str(&format!(
                    "Failed to queue download job for {}: {}\n",
                    url, message
                ));
            }
            None => {
                success_count += 1;
            }
        }
    }
    response.push_str(&format!(
        "Summary: {} queued, {} failed to queue",
        success_count, failure_count
    ));
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

const ACTION_SHOW_FAILED: &str = "show_failed_downloads";
const ACTION_RETRY_FAILED: &str = "retry_failed_downloads";

struct SharedActionState {
    pub allowed_user: teloxide::types::UserId,
    pub handle_requests: Arc<dyn HandleTelegramBotRequests + Send + Sync>,
}

fn action_keyboard() -> teloxide::types::InlineKeyboardMarkup {
    teloxide::types::InlineKeyboardMarkup::new(vec![vec![
        teloxide::types::InlineKeyboardButton::callback(
            "Show failed downloads",
            ACTION_SHOW_FAILED,
        ),
        teloxide::types::InlineKeyboardButton::callback("Retry", ACTION_RETRY_FAILED),
    ]])
}

pub async fn process_callback_query(
    bot: Bot,
    query: teloxide::types::CallbackQuery,
    allowed_user: &teloxide::types::UserId,
    handle_requests: &(dyn HandleTelegramBotRequests + Send + Sync),
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if query.from.id != *allowed_user {
        warn!(
            "User {:?} (ID: {}) is not allowed to use this bot, ignoring callback",
            query.from.username, query.from.id
        );
        return Ok(());
    }

    let response = match query.data.as_deref() {
        Some(ACTION_SHOW_FAILED) => {
            let failed = handle_requests.list_failed_downloads().await;
            if failed.is_empty() {
                "No failed downloads.".to_string()
            } else {
                format!("Failed downloads:\n{}", failed.join("\n"))
            }
        }
        Some(ACTION_RETRY_FAILED) => {
            let (queued, failed) = handle_requests.retry_failed_downloads().await;
            format!("Retry summary: {} queued, {} still failing", queued, failed)
        }
        _ => "Unknown action.".to_string(),
    };

    bot.send_message(query.from.id, response)
        .reply_markup(action_keyboard())
        .await?;

    bot.answer_callback_query(query.id).await?;
    Ok(())
}

impl TeloxideTelegramBot {
    pub async fn run_with_buttons(
        &self,
        handle_requests: Arc<dyn HandleTelegramBotRequests + Send + Sync>,
    ) {
        info!("Starting Telegram bot with buttons...");
        let bot = Bot::new(&self.telegram_api_token);
        let state = Arc::new(SharedActionState {
            allowed_user: self.allowed_user,
            handle_requests,
        });

        let handler =
            dptree::entry()
                .branch(Update::filter_message().endpoint(
                    |bot: Bot, state: Arc<SharedActionState>, msg: Message| async move {
                        let chat_id = msg.chat.id;
                        let handle_requests = state.handle_requests.clone();

                        process_message(
                            bot.clone(),
                            msg,
                            &state.allowed_user,
                            handle_requests.as_ref(),
                        )
                        .await?;

                        bot.send_message(chat_id, "Actions:")
                            .reply_markup(action_keyboard())
                            .await?;
                        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                    },
                ))
                .branch(Update::filter_callback_query().endpoint(
                    |bot: Bot,
                     state: Arc<SharedActionState>,
                     query: teloxide::types::CallbackQuery| async move {
                        let handle_requests = state.handle_requests.clone();
                        process_callback_query(
                            bot,
                            query,
                            &state.allowed_user,
                            handle_requests.as_ref(),
                        )
                        .await
                    },
                ));

        Dispatcher::builder(bot, handler)
            .dependencies(dptree::deps![state])
            .build()
            .dispatch()
            .await;

        info!("Telegram bot with buttons stopped.");
    }
}

#[async_trait::async_trait]
impl TelegramBot for TeloxideTelegramBot {
    async fn run(&self, handle_requests: Arc<dyn HandleTelegramBotRequests + Send + Sync>) {
        self.run_with_buttons(handle_requests).await;
    }
}
