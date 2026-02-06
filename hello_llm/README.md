# Hello LLM

A minimal example binary that demonstrates local LLM inference using the `llama-cpp-2` crate.

## Quick Start

Just run the binary - it will automatically download Phi-3 Mini if the model doesn't exist:

```bash
cargo run --bin hello_llm --release
```

## Model Download

The program will automatically attempt to download Phi-3 Mini (Q4_K_M quantized, ~2.3 GB) from Hugging Face if `models/model.gguf` doesn't exist.

### Manual Download (if automatic download fails)

If the automatic download fails (e.g., no internet connection), you can manually download a model:

1. Download Phi-3 Mini from [Hugging Face](https://huggingface.co/microsoft/Phi-3-mini-4k-instruct-gguf)
2. Place the model file at: `models/model.gguf` (relative to repository root)
3. Run the binary

You can use any GGUF format model from sources like:
- [Hugging Face GGUF models](https://huggingface.co/models?library=gguf)
- Other sources that provide GGUF format models

## What it does

The program:
1. Checks if `models/model.gguf` exists, downloads Phi-3 Mini if not
2. Initializes the llama backend
3. Loads the model
4. Creates a context with default parameters
5. Runs the prompt: "In one short sentence, what is Rust?"
6. Generates up to 100 tokens of response using greedy sampling
7. Prints the response to stdout

## Implementation details

- Uses synchronous, blocking API (no async)
- Uses default model and context parameters
- Uses greedy sampling for deterministic output
- No GPU acceleration (CPU only)
- Minimal error handling with `expect()` calls
- Automatic model download with progress indicator

## Example output

```
Model found at models/model.gguf
Prompt: In one short sentence, what is Rust?
Generating response...

Rust is a systems programming language focused on safety, speed, and concurrency.
```

(Note: Actual output will vary depending on the model used)

