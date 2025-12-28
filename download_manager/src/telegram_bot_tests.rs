use crate::telegram_bot::{
    process_message_impl, split_message_into_urls, HandleTelegramBotRequests,
    ProcessMessageResultingAction,
};
use pretty_assertions::assert_eq;

#[test_log::test]
fn test_split_message_into_urls() {
    assert_eq!(split_message_into_urls(""), Vec::<&str>::new());
    assert_eq!(split_message_into_urls(" \n "), Vec::<&str>::new());
    assert_eq!(
        split_message_into_urls("http://example.com/file1 http://example.com/file2"),
        vec!["http://example.com/file1", "http://example.com/file2"]
    );
    assert_eq!(
        split_message_into_urls("http://example.com/file1\nhttp://example.com/file2"),
        vec!["http://example.com/file1", "http://example.com/file2"]
    );
    assert_eq!(
        split_message_into_urls("http://example.com/file1\r\n\thttp://example.com/file2\r\n"),
        vec!["http://example.com/file1", "http://example.com/file2"]
    );
    assert_eq!(
        split_message_into_urls("http://example.com/file1\n\nhttp://example.com/file2"),
        vec!["http://example.com/file1", "http://example.com/file2"]
    );
}

struct FakeHandleRequests {}

#[async_trait::async_trait]
impl HandleTelegramBotRequests for FakeHandleRequests {
    async fn add_download_job(&self, url: &str) -> Option<String> {
        assert_eq!("http://example.com", url);
        None
    }
}

#[test_log::test(tokio::test)]
async fn test_process_message_impl() {
    let handle_requests = FakeHandleRequests {};
    let action = process_message_impl("http://example.com", &handle_requests)
        .await
        .unwrap();
    assert_eq!(
        action,
        ProcessMessageResultingAction::SendMessage(
            "Queued download job for http://example.com\n\nSummary: 1 succeeded, 0 failed".into()
        )
    );
}
