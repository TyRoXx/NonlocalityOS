use crate::expressions::{
    deserialize_recursively, evaluate, serialize_recursively, DeepExpression, Expression,
    PrintExpression,
};
use astraea::{deep_tree::DeepTree, storage::InMemoryTreeStorage};
use pretty_assertions::assert_eq;
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn effect() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let empty_tree = Arc::new(DeepExpression(Expression::make_literal(DeepTree::empty())));
    let first_console_output = crate::standard_library::ConsoleOutput {
        message: DeepTree::try_from_string("Hello, ").unwrap(),
    };
    let first_console_output_tree = first_console_output.to_tree();
    let first_console_output_expression =
        DeepExpression(Expression::make_literal(first_console_output_tree.clone()));

    let second_string = DeepTree::try_from_string(" world!\n").unwrap();
    let second_console_output_expression =
        DeepExpression(Expression::ConstructTree(vec![Arc::new(DeepExpression(
            Expression::make_environment(),
        ))]));

    let and_then_lambda_expression = DeepExpression(Expression::make_lambda(
        empty_tree.clone(),
        Arc::new(second_console_output_expression),
    ));

    let construct_and_then_expression = DeepExpression(Expression::make_construct_tree(vec![
        Arc::new(first_console_output_expression),
        Arc::new(and_then_lambda_expression),
    ]));

    let main_lambda_expression = DeepExpression(Expression::make_lambda(
        empty_tree.clone(),
        Arc::new(construct_and_then_expression),
    ));
    {
        let mut program_as_string = String::new();
        main_lambda_expression
            .0
            .print(&mut program_as_string, 0)
            .unwrap();
        assert_eq!(concat!(
            "$env={literal(DeepTree { blob: TreeBlob { content.len(): 0 }, references: [] })}($arg) =>\n",
            "  [literal(DeepTree { blob: TreeBlob { content.len(): 0 }, references: [DeepTree { blob: TreeBlob { content.len(): 7 }, references: [] }] }), $env={literal(DeepTree { blob: TreeBlob { content.len(): 0 }, references: [] })}($arg) =>\n",
            "    [$env, ], ]"),
            program_as_string.as_str());
    }
    let call_main = DeepExpression(Expression::make_apply(
        Arc::new(main_lambda_expression),
        Arc::new(DeepExpression(Expression::make_literal(second_string))),
    ));

    // verify that this complex expression roundtrips through serialization and deserialization correctly
    let call_main_digest = serialize_recursively(&call_main, &*storage).await.unwrap();
    let deserialized_call_main = deserialize_recursively(&call_main_digest, &*storage)
        .await
        .unwrap();
    assert_eq!(call_main, deserialized_call_main);
    assert_eq!(
        concat!(
            "cfc5e5a5af2a776b7e68af66ee9fcaf1a6d60a8a6c7c83662559721486640e7c",
            "42ff89a67c184be5c7aac78ac674f778b8e620b29ac2dc9775ad6e162ea212ab"
        ),
        format!("{}", &call_main_digest)
    );

    let main_result = evaluate(&call_main, &*storage, &*storage, &None, &None)
        .await
        .unwrap();
    assert_eq!(
        concat!(
            "4bcb4ead6334a387f95af13a11a6f33497ddead7689574c07072c11433313324",
            "c22ab666038872a20f139846489494249545d0aed3b2d8042071e5aeacc45dd2"
        ),
        format!("{}", &main_result)
    );
}
