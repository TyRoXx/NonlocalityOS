# Hello LLM

A minimal example binary that demonstrates local LLM inference using the `llama-cpp-2` crate.

## Prerequisites

Before running this binary, you need a GGUF model file. Place your model file at:

```
models/model.gguf
```

(relative to the repository root)

You can download GGUF models from:
- [Hugging Face](https://huggingface.co/models?library=gguf)
- Other sources that provide GGUF format models

## Building

To build the binary in release mode:

```bash
cargo build --bin hello_llm --release
```

## Running

To run the binary:

```bash
cargo run --bin hello_llm --release
```

Or directly execute the built binary:

```bash
./target/release/hello_llm
```

## What it does

The program:
1. Initializes the llama backend
2. Loads the model from `models/model.gguf`
3. Creates a context with default parameters
4. Runs the prompt: "In one short sentence, what is Rust?"
5. Generates up to 100 tokens of response using greedy sampling
6. Prints the response to stdout

## Implementation details

- Uses synchronous, blocking API (no async)
- Uses default model and context parameters
- Uses greedy sampling for deterministic output
- No GPU acceleration (CPU only)
- Minimal error handling with `expect()` calls

## Example output

```
Prompt: In one short sentence, what is Rust?
Generating response...

Rust is a systems programming language focused on safety, speed, and concurrency.
```

(Note: Actual output will vary depending on the model used)
