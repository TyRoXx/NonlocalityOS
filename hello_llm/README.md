# Hello LLM

A minimal example binary that demonstrates local LLM inference using the `llama-cpp-2` crate.

## Quick Start

Just run the binary - it will automatically download Phi-3 Mini if the model doesn't exist:

```bash
cargo run --bin hello_llm --release
```

## Model Download

The program will automatically attempt to download Phi-3 Mini (Q4_K_M quantized, ~2.3 GB) from Hugging Face if `models/model.gguf` doesn't exist.

**Security**: The downloaded file is verified using SHA256 hash (`8a83c7fb9049a9b2e92266fa7ad04933bb53aa1e85136b7b30f1b8000ff2edef`) to ensure integrity. If verification fails, the file is deleted and an error is returned.

### Manual Download (if automatic download fails)

If the automatic download fails (e.g., no internet connection), you can manually download the model:

1. Download Phi-3 Mini Q4_K_M from [Hugging Face](https://huggingface.co/microsoft/Phi-3-mini-4k-instruct-gguf/blob/main/Phi-3-mini-4k-instruct-q4.gguf)
2. Rename it to `model.gguf` and place it at: `models/model.gguf` (relative to repository root)
3. Run the binary

You can also use any other GGUF format model from sources like:
- [Hugging Face GGUF models](https://huggingface.co/models?library=gguf)
- Other sources that provide GGUF format models

## What it does

The program:
1. Checks if `models/model.gguf` exists, downloads Phi-3 Mini if not
2. Verifies downloaded file integrity using SHA256 hash
3. Initializes the llama backend
4. Loads the model
5. Creates a context with default parameters
6. Runs the prompt: "In one short sentence, what is Rust?"
7. Generates up to 100 tokens of response using greedy sampling
8. Prints the response to stdout

## Implementation details

- Uses synchronous, blocking API (no async)
- Uses default model and context parameters
- Uses greedy sampling for deterministic output
- No GPU acceleration (CPU only)
- Minimal error handling with `expect()` calls
- Automatic model download with progress indicator
- SHA256 hash verification for downloaded files

## Example output

```
Model found at models/model.gguf
Prompt: In one short sentence, what is Rust?
Generating response...

Rust is a systems programming language focused on safety, speed, and concurrency.
```

(Note: Actual output will vary depending on the model used)

