use crate::telegram_bot::{
    process_message_impl, HandleTelegramBotRequests, ProcessMessageResultingAction,
};
use pretty_assertions::assert_eq;

struct FakeHandleRequests {}

#[async_trait::async_trait]
impl HandleTelegramBotRequests for FakeHandleRequests {
    async fn add_download_job(&self, url: &str) -> Option<String> {
        assert_eq!("", url);
        None
    }
}

#[test_log::test(tokio::test)]
async fn test_process_message_impl() {
    let handle_requests = FakeHandleRequests {};
    let action = process_message_impl("", &handle_requests).await.unwrap();
    assert_eq!(
        action,
        ProcessMessageResultingAction::SendMessage("Successfully added download job".into())
    );
}
