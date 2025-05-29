use crate::expressions::{evaluate, DeepExpression, Expression, PrintExpression};
use astraea::{deep_tree::DeepTree, storage::InMemoryTreeStorage};
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn hello_world() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let empty_tree = Arc::new(DeepExpression(Expression::make_literal(DeepTree::empty())));
    let hello_world_string = DeepTree::try_from_string("Hello, world!\n").unwrap();
    let console_output = crate::standard_library::ConsoleOutput {
        message: hello_world_string,
    };
    let console_output_tree = console_output.to_tree();
    let console_output_expression =
        DeepExpression(Expression::make_literal(console_output_tree.clone()));
    let lambda_expression = DeepExpression(Expression::make_lambda(
        empty_tree,
        Arc::new(console_output_expression),
    ));
    {
        let mut program_as_string = String::new();
        lambda_expression
            .0
            .print(&mut program_as_string, 0)
            .unwrap();
        assert_eq!(
            concat!(
            "$env={literal(DeepTree { blob: TreeBlob { content.len(): 0 }, references: [] })}($arg) =>\n",
            "  literal(DeepTree { blob: TreeBlob { content.len(): 0 }, references: [DeepTree { blob: TreeBlob { content.len(): 14 }, references: [] }] })"),
            program_as_string.as_str()
        );
    }
    let call_main = DeepExpression(Expression::make_apply(
        Arc::new(lambda_expression),
        Arc::new(DeepExpression(Expression::make_literal(DeepTree::empty()))),
    ));
    let main_result = evaluate(&call_main, &*storage, &*storage, &None, &None)
        .await
        .unwrap();
    let serialized_result = DeepTree::deserialize(&main_result, &*storage)
        .await
        .unwrap();
    let deserialized_result =
        crate::standard_library::ConsoleOutput::from_tree(&serialized_result).unwrap();
    assert_eq!(&console_output, &deserialized_result);
}
