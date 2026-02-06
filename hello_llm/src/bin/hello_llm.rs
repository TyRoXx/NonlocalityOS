use llama_cpp_2::{
    context::params::LlamaContextParams,
    llama_backend::LlamaBackend,
    llama_batch::LlamaBatch,
    model::{params::LlamaModelParams, LlamaModel, AddBos},
    sampling::LlamaSampler,
};

fn main() {
    // Initialize the llama backend
    let backend = LlamaBackend::init()
        .expect("Failed to initialize llama backend");

    // Load the model from the specified path
    let model_path = "models/model.gguf";
    let model_params = LlamaModelParams::default();
    let model = LlamaModel::load_from_file(&backend, model_path, &model_params)
        .expect("Failed to load model");

    // Create a context with default parameters
    let ctx_params = LlamaContextParams::default();
    let mut ctx = model
        .new_context(&backend, ctx_params)
        .expect("Failed to create context");

    // The prompt we want to run
    let prompt = "In one short sentence, what is Rust?";
    
    // Tokenize the prompt
    let tokens = model
        .str_to_token(prompt, AddBos::Always)
        .expect("Failed to tokenize prompt");

    println!("Prompt: {}", prompt);
    println!("Generating response...\n");

    // Create a batch and add tokens
    let mut batch = LlamaBatch::new(512, 1);
    
    let last_index = tokens.len() - 1;
    for (i, token) in tokens.iter().enumerate() {
        // Only compute logits for the last token
        let is_last = i == last_index;
        batch.add(*token, i as i32, &[0], is_last)
            .expect("Failed to add token to batch");
    }

    // Decode the batch (process the prompt)
    ctx.decode(&mut batch)
        .expect("Failed to decode batch");

    // Create a simple greedy sampler
    let mut sampler = LlamaSampler::greedy();

    // Create a UTF-8 decoder for converting tokens to strings
    let mut decoder = encoding_rs::UTF_8.new_decoder();

    // Generate tokens
    let mut response = String::new();
    let max_tokens = 100;
    
    for _ in 0..max_tokens {
        // Sample the next token
        let new_token = sampler.sample(&ctx, -1);
        
        // Check if we've reached the end of generation
        if model.is_eog_token(new_token) {
            break;
        }

        // Accept the token for the sampler state
        sampler.accept(new_token);

        // Convert token to string and append to response
        let token_str = model
            .token_to_piece(new_token, &mut decoder, false, None)
            .expect("Failed to convert token to string");
        response.push_str(&token_str);

        // Clear batch and add the new token
        batch.clear();
        batch.add(new_token, 0, &[0], true)
            .expect("Failed to add token to batch");

        // Decode the new token
        ctx.decode(&mut batch)
            .expect("Failed to decode batch");
    }

    println!("{}", response.trim());
}
