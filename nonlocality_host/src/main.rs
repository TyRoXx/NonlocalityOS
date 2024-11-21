use astraea::storage::InMemoryValueStorage;
use astraea::storage::StoreValue;
use astraea::tree::HashedValue;
use astraea::tree::TypeId;
use astraea::tree::Value;
use astraea::tree::TYPE_ID_CONSOLE;
use astraea::tree::TYPE_ID_EFFECT;
use astraea::tree::TYPE_ID_SECONDS;
use astraea::tree::TYPE_ID_STRING;
use lambda::expressions::make_beginning_of_time;
use lambda::expressions::make_text_in_console;
use lambda::expressions::reduce_expression_without_storing_the_final_result;
use lambda::expressions::ActualConsole;
use lambda::expressions::Identity;
use lambda::expressions::ReduceExpression;
use lambda::expressions::ServiceRegistry;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

async fn run_host() -> std::io::Result<()> {
    let test_console: Arc<dyn ReduceExpression> = Arc::new(ActualConsole {});
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry::new(BTreeMap::from([
        (TYPE_ID_STRING, identity.clone()),
        (TypeId(1), identity.clone()),
        (TYPE_ID_CONSOLE, test_console),
        (TYPE_ID_EFFECT, identity.clone()),
        (TYPE_ID_SECONDS, identity),
    ]));
    let value_storage = InMemoryValueStorage::new(Mutex::new(BTreeMap::new()));
    let past = value_storage
        .store_value(&HashedValue::from(Arc::new(make_beginning_of_time())))
        .await
        .unwrap();
    let message_1 = value_storage
        .store_value(&HashedValue::from(Arc::new(
            Value::from_string("hello, ").unwrap(),
        )))
        .await
        .unwrap();
    let text_in_console_1 = value_storage
        .store_value(&HashedValue::from(Arc::new(
            make_text_in_console(past, message_1).value,
        )))
        .await
        .unwrap();
    let message_2 = value_storage
        .store_value(&HashedValue::from(Arc::new(
            Value::from_string("world!\n").unwrap(),
        )))
        .await
        .unwrap();
    let text_in_console_2 = make_text_in_console(text_in_console_1, message_2);
    let _result = reduce_expression_without_storing_the_final_result(
        text_in_console_2,
        &services,
        &value_storage,
        &value_storage,
    )
    .await
    .unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_run_host() -> std::io::Result<()> {
    run_host().await
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    run_host().await
}
