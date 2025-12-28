use crate::telegram_bot::{
    process_message_impl, split_message_into_urls, HandleTelegramBotRequests,
    ProcessMessageResultingAction,
};
use pretty_assertions::assert_eq;
use std::sync::Arc;
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
        ProcessMessageResultingAction::SendMessage(
            "Queued download job for http://example.com\n\nSummary: 1 succeeded, 0 failed".into()
        )
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
        ProcessMessageResultingAction::SendMessage(
            "Queued download job for http://example.com/file1\nQueued download job for http://example.com/file2\n\nSummary: 2 succeeded, 0 failed".into()
        )
    );
    handle_requests.assert_complete().await;
}

#[test_log::test(tokio::test)]
async fn test_process_message_impl_failure() {
    let handle_requests = FakeHandleRequests {
        expected_download_jobs: vec![
            ("http://example.com/file1", None),
            ("http://example.com/file2", Some("Test error")),
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
        ProcessMessageResultingAction::SendMessage(
            "Queued download job for http://example.com/file1\nFailed to queue download job for http://example.com/file2: Test error\n\nSummary: 1 succeeded, 1 failed".into()
        )
    );
    handle_requests.assert_complete().await;
}
