use llama_cpp_2::{
    context::params::LlamaContextParams,
    llama_backend::LlamaBackend,
    llama_batch::LlamaBatch,
    model::{params::LlamaModelParams, AddBos, LlamaModel},
    sampling::LlamaSampler,
};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

/// Download Phi-3 Mini model if it doesn't exist
fn ensure_model_exists(model_path: &str) -> io::Result<()> {
    if Path::new(model_path).exists() {
        println!("Model found at {}", model_path);
        return Ok(());
    }

    println!("Model not found at {}", model_path);
    println!("Attempting to download Phi-3 Mini (Q4_K_M quantized, ~2.3 GB)...");
    println!("This may take a few minutes...\n");

    // Expected SHA256 hash for Phi-3 Mini Q4_K_M
    let expected_hash = "8a83c7fb9049a9b2e92266fa7ad04933bb53aa1e85136b7b30f1b8000ff2edef";

    // Phi-3 Mini 4K Instruct GGUF Q4_K_M from Hugging Face
    let model_url = "https://huggingface.co/microsoft/Phi-3-mini-4k-instruct-gguf/resolve/main/Phi-3-mini-4k-instruct-q4.gguf";

    // Create parent directory if it doesn't exist
    if let Some(parent) = Path::new(model_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Download the model
    let mut response = ureq::get(model_url).call().map_err(|e| {
        eprintln!("\nFailed to download model: {}", e);
        eprintln!("\nPlease manually download a GGUF model file and place it at:");
        eprintln!("  {}", model_path);
        eprintln!("\nYou can download Phi-3 Mini from:");
        eprintln!("  https://huggingface.co/microsoft/Phi-3-mini-4k-instruct-gguf");
        eprintln!("\nOr use any other GGUF format model.");
        io::Error::other(format!("Download failed: {}", e))
    })?;

    let total_size = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Write to a temporary file first, then rename atomically
    let temp_path = format!("{}.tmp", model_path);
    let mut file = File::create(&temp_path)?;
    let mut reader = response.body_mut().as_reader();
    let mut buffer = [0; 8192];
    let mut downloaded = 0u64;
    let mut hasher = Sha256::new();

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        file.write_all(&buffer[..bytes_read])?;
        hasher.update(&buffer[..bytes_read]);
        downloaded += bytes_read as u64;

        if total_size > 0 {
            let progress = (downloaded as f64 / total_size as f64) * 100.0;
            print!(
                "\rProgress: {:.1}% ({} MB / {} MB)",
                progress,
                downloaded / 1_048_576,
                total_size / 1_048_576
            );
            io::stdout().flush()?;
        }
    }

    // Flush to ensure all data is written to disk
    file.flush()?;
    drop(file); // Close the file before renaming

    // Compute the final hash
    let hash_result = hasher.finalize();
    let computed_hash = format!("{:x}", hash_result);

    println!("\n\nModel downloaded successfully");
    println!("Verifying SHA256 hash...");
    println!("Computed: {}", computed_hash);
    println!("Expected: {}", expected_hash);

    if computed_hash != expected_hash {
        // Delete the temporary file if hash verification fails
        let _ = std::fs::remove_file(&temp_path);
        return Err(io::Error::other(format!(
            "Hash verification failed! Computed hash {} does not match expected hash {}. The temporary file has been deleted.",
            computed_hash, expected_hash
        )));
    }

    println!("Hash verification successful!");

    // Atomically rename the temporary file to the final destination
    std::fs::rename(&temp_path, model_path)?;
    println!("Model saved to {}", model_path);

    Ok(())
}

fn main() {
    // Load the model from the specified path
    let model_path = "models/model.gguf";

    // Ensure the model exists, download if necessary
    ensure_model_exists(model_path).expect("Failed to ensure model exists");

    // Initialize the llama backend
    let backend = LlamaBackend::init().expect("Failed to initialize llama backend");
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
        batch
            .add(*token, i as i32, &[0], is_last)
            .expect("Failed to add token to batch");
    }

    // Decode the batch (process the prompt)
    ctx.decode(&mut batch).expect("Failed to decode batch");

    // Create a simple greedy sampler
    let mut sampler = LlamaSampler::greedy();

    // Create a UTF-8 decoder for converting tokens to strings
    let mut decoder = encoding_rs::UTF_8.new_decoder();

    // Generate tokens
    let mut response = String::new();
    let max_tokens = 300;

    // Track the position for new tokens (starts after the prompt)
    let mut n_past = tokens.len() as i32;

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

        // Clear batch and add the new token with the correct position
        batch.clear();
        batch
            .add(new_token, n_past, &[0], true)
            .expect("Failed to add token to batch");

        // Increment position counter
        n_past += 1;

        // Decode the new token
        ctx.decode(&mut batch).expect("Failed to decode batch");
    }

    println!("{}", response.trim());
}
