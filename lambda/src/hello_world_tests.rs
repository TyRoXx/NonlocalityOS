use crate::expressions::{evaluate, DeepExpression, Expression, PrintExpression};
use astraea::{
    storage::{InMemoryTreeStorage, LoadTree, StoreTree},
    tree::{HashedTree, Tree},
};
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn hello_world() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let empty_tree = Arc::new(DeepExpression(Expression::make_literal(Tree::empty())));
    let hello_world_string = Arc::new(Tree::from_string("Hello, world!\n").unwrap());
    let hello_world_string_ref = storage
        .store_tree(&HashedTree::from(hello_world_string))
        .await
        .unwrap();
    let console_output = crate::standard_library::ConsoleOutput {
        message: hello_world_string_ref,
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
            "$env={literal(Tree { blob: TreeBlob { content.len(): 0 }, references: [] })}($arg) =>\n",
            "  literal(Tree { blob: TreeBlob { content.len(): 0 }, references: [BlobDigest(\"d15b55538dda31e32a571857fe83d5cbb6a2e06bdc5b65ac88fed7827df359be7cefeef68b0a52023695d9f62eab69ea72b74fb7f89fd6974130454164333290\")] })"),
            program_as_string.as_str()
        );
    }
    let call_main = DeepExpression(Expression::make_apply(
        Arc::new(lambda_expression),
        Arc::new(DeepExpression(Expression::make_literal(Tree::empty()))),
    ));
    let main_result = evaluate(&call_main, &*storage, &*storage, &None)
        .await
        .unwrap();
    let serialized_result = storage
        .load_tree(&main_result)
        .await
        .unwrap()
        .hash()
        .unwrap();
    let deserialized_result =
        crate::standard_library::ConsoleOutput::from_tree(serialized_result.tree()).unwrap();
    assert_eq!(&console_output, &deserialized_result);
}
