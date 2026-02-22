use std::sync::Arc;
use teloxide::{
    dispatching::UpdateFilterExt,
    dptree,
    payloads::SendMessageSetters,
    prelude::{Dispatcher, Requester},
    sugar::request::RequestLinkPreviewExt,
    types::{ChatId, Message, Update, User},
    Bot,
};
use tracing::{info, warn};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AddDownloadJobOutcome {
    New,
    Duplicate,
    Error(String),
}

#[async_trait::async_trait]
pub trait HandleTelegramBotRequests {
    async fn add_download_job(&self, url: &str) -> AddDownloadJobOutcome;
    async fn list_failed_downloads(&self) -> Result<Vec<(String, u32)>, String>;
    async fn retry_failed_downloads(&self) -> Option<u64>;
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
    let mut new_count = 0;
    let mut duplicate_count = 0;
    let mut failure_count = 0;
    for url in urls {
        let outcome = handle_requests.add_download_job(url).await;
        match outcome {
            AddDownloadJobOutcome::New => {
                new_count += 1;
            }
            AddDownloadJobOutcome::Duplicate => {
                duplicate_count += 1;
            }
            AddDownloadJobOutcome::Error(message) => {
                failure_count += 1;
                response.push_str(&format!(
                    "Failed to queue download job for {}: {}\n",
                    url, message
                ));
            }
        }
    }
    response.push_str(&format!(
        "Summary: {} new URLs queued, {} duplicates ignored, {} failed to queue",
        new_count, duplicate_count, failure_count
    ));
    Ok(ProcessMessageResultingAction::SendMessage(response))
}

pub fn is_authorized_user(user: &Option<User>, allowed_user: &teloxide::types::UserId) -> bool {
    match user {
        Some(user) => user.id == *allowed_user,
        None => false,
    }
}

fn check_authorization(user: &Option<User>, allowed_user: &teloxide::types::UserId) -> bool {
    if is_authorized_user(user, allowed_user) {
        true
    } else {
        warn!(
            "User {:?} (ID: {:?}) is not allowed to use this bot, ignoring message",
            user.as_ref().map(|u| u.username.clone()),
            user.as_ref().map(|u| u.id)
        );
        false
    }
}

pub async fn process_message(
    bot: Bot,
    message_text: &Option<&str>,
    chat: &ChatId,
    handle_requests: &(dyn HandleTelegramBotRequests + Send + Sync),
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match message_text {
        Some(text) => {
            let action = process_message_impl(text, handle_requests).await?;
            match action {
                ProcessMessageResultingAction::SendMessage(response) => {
                    bot.send_message(*chat, response).await?;
                }
            }
        }
        None => {
            bot.send_message(*chat, "Received message without text, ignoring.")
                .await?;
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
    query_data: &Option<&str>,
    chat: &ChatId,
    handle_requests: &(dyn HandleTelegramBotRequests + Send + Sync),
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let response = match query_data {
        Some(ACTION_SHOW_FAILED) => match handle_requests.list_failed_downloads().await {
            Ok(failed) => {
                if failed.is_empty() {
                    "No failed downloads.".to_string()
                } else {
                    format!(
                        "Failed downloads:\n{}",
                        failed
                            .iter()
                            .map(|(url, fail_count)| format!("{} ({} failures)", url, fail_count))
                            .collect::<Vec<_>>()
                            .join("\n")
                    )
                }
            }
            Err(e) => {
                format!("Failed to list failed downloads: {}", e)
            }
        },
        Some(ACTION_RETRY_FAILED) => match handle_requests.retry_failed_downloads().await {
            Some(retried_count) => format!("Retrying {} failed downloads.", retried_count),
            None => "Failed to retry failed downloads.".to_string(),
        },
        _ => "Unknown action.".to_string(),
    };

    bot.send_message(*chat, response)
        .disable_link_preview(true)
        .reply_markup(action_keyboard())
        .await?;

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
                        if !check_authorization(&msg.from, &state.allowed_user) {
                            return Ok(());
                        }

                        let handle_requests = state.handle_requests.clone();
                        process_message(
                            bot.clone(),
                            &msg.text(),
                            &msg.chat.id,
                            handle_requests.as_ref(),
                        )
                        .await?;

                        bot.send_message(msg.chat.id, "Actions:")
                            .reply_markup(action_keyboard())
                            .await?;
                        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                    },
                ))
                .branch(Update::filter_callback_query().endpoint(
                    |bot: Bot,
                     state: Arc<SharedActionState>,
                     query: teloxide::types::CallbackQuery| async move {
                        if !check_authorization(&Some(query.from.clone()), &state.allowed_user) {
                            return Ok(());
                        }

                        // Always dismiss the callback spinner, even if the follow-up message fails.
                        bot.answer_callback_query(query.id.clone()).await?;

                        match query.message {
                            Some(ref message) => {
                                let handle_requests = state.handle_requests.clone();
                                process_callback_query(
                                    bot,
                                    &query.data.as_deref(),
                                    &message.chat().id,
                                    handle_requests.as_ref(),
                                )
                                .await
                            }
                            None => {
                                warn!(
                                    "Received callback query without message, ignoring. Query data: {:?}",
                                    query.data
                                );
                                Ok(())
                            }
                        }
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
