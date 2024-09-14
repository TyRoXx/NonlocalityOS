#![feature(array_chunks)]
use astraea::tree::make_beginning_of_time;
use astraea::tree::make_delay;
use astraea::tree::make_seconds;
use astraea::tree::make_text_in_console;
use astraea::tree::reduce_expression_without_storing_the_final_result;
use astraea::tree::ActualConsole;
use astraea::tree::DelayService;
use astraea::tree::Identity;
use astraea::tree::InMemoryValueStorage;
use astraea::tree::ReduceExpression;
use astraea::tree::ServiceRegistry;
use astraea::tree::StoreValue;
use astraea::tree::TypeId;
use astraea::tree::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    let test_console: Arc<dyn ReduceExpression> = Arc::new(ActualConsole {});
    let delay_service: Arc<dyn ReduceExpression> = Arc::new(DelayService {});
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry::new(BTreeMap::from([
        (TypeId(0), identity.clone()),
        (TypeId(1), identity.clone()),
        (TypeId(2), test_console),
        (TypeId(3), identity.clone()),
        (TypeId(4), delay_service),
        (TypeId(5), identity),
    ]));
    let value_storage = InMemoryValueStorage::new(Mutex::new(BTreeMap::new()));
    let past = value_storage
        .store_value(Arc::new(make_beginning_of_time()))
        .add_type(TypeId(3));
    let message_1 = value_storage
        .store_value(Arc::new(Value::from_string("hello, ")))
        .add_type(TypeId(0));
    let text_in_console_1 = value_storage
        .store_value(Arc::new(make_text_in_console(past, message_1).value))
        .add_type(TypeId(2));
    let duration = value_storage
        .store_value(Arc::new(make_seconds(3).value))
        .add_type(TypeId(5));
    let delay = value_storage
        .store_value(Arc::new(make_delay(text_in_console_1, duration).value))
        .add_type(TypeId(4));
    let message_2 = value_storage
        .store_value(Arc::new(Value::from_string("world!\n")))
        .add_type(TypeId(0));
    let text_in_console_2 = make_text_in_console(delay, message_2);
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
