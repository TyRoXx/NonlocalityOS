use crate::telegram_bot::{
    is_authorized_user, process_message_impl, split_message_into_urls, HandleTelegramBotRequests,
    ProcessMessageResultingAction,
};
use pretty_assertions::assert_eq;
use std::sync::Arc;
use teloxide::types::User;
use tokio::sync::Mutex;

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

struct FakeHandleRequestsMutableState {
    download_jobs_added: usize,
}

struct FakeHandleRequests {
    expected_download_jobs: Vec<(&'static str, Option<&'static str>)>,
    mutable_state: Arc<Mutex<FakeHandleRequestsMutableState>>,
}

impl FakeHandleRequests {
    pub async fn assert_complete(&self) {
        let locked = self.mutable_state.lock().await;
        assert_eq!(
            locked.download_jobs_added,
            self.expected_download_jobs.len()
        );
    }
}

#[async_trait::async_trait]
impl HandleTelegramBotRequests for FakeHandleRequests {
    async fn add_download_job(&self, url: &str) -> Option<String> {
        let mut locked = self.mutable_state.lock().await;
        let expectation = self.expected_download_jobs.get(locked.download_jobs_added);
        locked.download_jobs_added = locked.download_jobs_added.checked_add(1).unwrap();
        match &expectation {
            Some((expected_url, response)) => {
                assert_eq!(url, *expected_url);
                response.map(|s| s.to_string())
            }
            None => panic!(
                "No more expected download jobs, but received request to add download job for {}",
                url
            ),
        }
    }

    async fn list_failed_downloads(&self) -> Result<Vec<(String, u32)>, String> {
        todo!()
    }

    async fn retry_failed_downloads(&self) -> Option<u64> {
        todo!()
    }
}

#[test_log::test(tokio::test)]
async fn test_process_message_impl_1() {
    let handle_requests = FakeHandleRequests {
        expected_download_jobs: vec![("http://example.com", None)],
        mutable_state: Arc::new(Mutex::new(FakeHandleRequestsMutableState {
            download_jobs_added: 0,
        })),
    };
    let action = process_message_impl("http://example.com", &handle_requests)
        .await
        .unwrap();
    assert_eq!(
        action,
        ProcessMessageResultingAction::SendMessage("Summary: 1 queued, 0 failed to queue".into())
    );
    handle_requests.assert_complete().await;
}

#[test_log::test(tokio::test)]
async fn test_process_message_impl_2() {
    let handle_requests = FakeHandleRequests {
        expected_download_jobs: vec![
            ("http://example.com/file1", None),
            ("http://example.com/file2", None),
        ],
        mutable_state: Arc::new(Mutex::new(FakeHandleRequestsMutableState {
            download_jobs_added: 0,
        })),
    };
    let action = process_message_impl(
        "http://example.com/file1\nhttp://example.com/file2",
        &handle_requests,
    )
    .await
    .unwrap();
    assert_eq!(
        action,
        ProcessMessageResultingAction::SendMessage("Summary: 2 queued, 0 failed to queue".into())
    );
    handle_requests.assert_complete().await;
}

#[test_log::test(tokio::test)]
async fn test_process_message_impl_failure() {
    let handle_requests = FakeHandleRequests {
        expected_download_jobs: vec![
            // the bot sorts URLs before queuing them
            ("http://example.com/file1", None),
            ("http://example.com/file2", Some("Test error 2")),
            ("http://example.com/file3", Some("Test error 3")),
        ],
        mutable_state: Arc::new(Mutex::new(FakeHandleRequestsMutableState {
            download_jobs_added: 0,
        })),
    };
    let action = process_message_impl(
        // queue out of order
        "http://example.com/file1\nhttp://example.com/file3\nhttp://example.com/file2",
        &handle_requests,
    )
    .await
    .unwrap();
    assert_eq!(
        action,
        ProcessMessageResultingAction::SendMessage(
            // errors are reported sorted by the URL
            "Failed to queue download job for http://example.com/file2: Test error 2\nFailed to queue download job for http://example.com/file3: Test error 3\nSummary: 1 queued, 2 failed to queue".into()
        )
    );
    handle_requests.assert_complete().await;
}

#[test_log::test]
fn test_is_authorized_user() {
    let allowed_user = teloxide::types::UserId(123);
    assert!(is_authorized_user(
        &Some(User {
            id: allowed_user,
            username: None,
            first_name: "".to_string(),
            last_name: None,
            is_bot: false,
            language_code: None,
            added_to_attachment_menu: false,
            is_premium: false,
        }),
        &allowed_user
    ));
    assert!(!is_authorized_user(
        &Some(User {
            id: teloxide::types::UserId(456),
            username: None,
            first_name: "".to_string(),
            last_name: None,
            is_bot: false,
            language_code: None,
            added_to_attachment_menu: false,
            is_premium: false,
        }),
        &allowed_user
    ));
    assert!(!is_authorized_user(&None, &allowed_user));
}
