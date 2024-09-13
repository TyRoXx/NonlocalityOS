#![feature(array_chunks)]
mod lib;
use lib::make_beginning_of_time;
use lib::make_delay;
use lib::make_seconds;
use lib::make_text_in_console;
use lib::reduce_expression_without_storing_the_final_result;
use lib::ActualConsole;
use lib::DelayService;
use lib::Identity;
use lib::InMemoryValueStorage;
use lib::ReduceExpression;
use lib::ServiceRegistry;
use lib::StoreValue;
use lib::TypeId;
use lib::Value;
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
