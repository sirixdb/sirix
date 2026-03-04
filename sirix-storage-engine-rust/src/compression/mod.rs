//! Byte handler pipeline for compression and decompression.
//!
//! Equivalent to Java's `ByteHandler` / `ByteHandlerPipeline`.
//! Supports composable chains of compression handlers.

use crate::error::Result;
use crate::error::StorageError;

/// A single compression/decompression handler.
pub trait ByteHandler: Send + Sync {
    /// Compress data into the output buffer.
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<()>;

    /// Decompress data into the output buffer.
    fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<()>;

    /// Handler name for diagnostics.
    fn name(&self) -> &str;
}

/// LZ4 compression handler using lz4_flex.
pub struct Lz4Handler;

impl Lz4Handler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Lz4Handler {
    fn default() -> Self {
        Self::new()
    }
}

impl ByteHandler for Lz4Handler {
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        // Format: [original_size: u32][compressed_data]
        let original_size = input.len() as u32;
        output.extend_from_slice(&original_size.to_le_bytes());

        let compressed = lz4_flex::compress_prepend_size(input);
        output.extend_from_slice(&compressed);
        Ok(())
    }

    fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        if input.len() < 4 {
            return Err(StorageError::Compression(
                "LZ4 input too short for size header".into(),
            ));
        }

        // Skip our original_size header
        let compressed = &input[4..];

        let decompressed = lz4_flex::decompress_size_prepended(compressed)
            .map_err(|e| StorageError::Compression(format!("LZ4 decompression failed: {e}")))?;

        output.extend_from_slice(&decompressed);
        Ok(())
    }

    fn name(&self) -> &str {
        "LZ4"
    }
}

/// Identity handler (no compression). Used when no compression is configured.
pub struct IdentityHandler;

impl ByteHandler for IdentityHandler {
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        output.extend_from_slice(input);
        Ok(())
    }

    fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        output.extend_from_slice(input);
        Ok(())
    }

    fn name(&self) -> &str {
        "Identity"
    }
}

/// A composable pipeline of byte handlers, applied in order for compression
/// and in reverse for decompression.
///
/// Equivalent to Java's `ByteHandlerPipeline`.
pub struct ByteHandlerPipeline {
    handlers: Vec<Box<dyn ByteHandler>>,
}

impl ByteHandlerPipeline {
    /// Create an empty pipeline (identity - no transformation).
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
        }
    }

    /// Create a pipeline with a single LZ4 handler.
    pub fn lz4() -> Self {
        let mut pipeline = Self::new();
        pipeline.add_handler(Box::new(Lz4Handler::new()));
        pipeline
    }

    /// Add a handler to the pipeline.
    pub fn add_handler(&mut self, handler: Box<dyn ByteHandler>) {
        self.handlers.push(handler);
    }

    /// Whether the pipeline is empty (identity transformation).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// Compress data through the pipeline (handlers applied in order).
    pub fn compress(&self, input: &[u8]) -> Result<Vec<u8>> {
        if self.handlers.is_empty() {
            return Ok(input.to_vec());
        }

        let mut current = input.to_vec();
        let mut next = Vec::new();

        for handler in &self.handlers {
            next.clear();
            handler.compress(&current, &mut next)?;
            std::mem::swap(&mut current, &mut next);
        }

        Ok(current)
    }

    /// Decompress data through the pipeline (handlers applied in reverse).
    pub fn decompress(&self, input: &[u8]) -> Result<Vec<u8>> {
        if self.handlers.is_empty() {
            return Ok(input.to_vec());
        }

        let mut current = input.to_vec();
        let mut next = Vec::new();

        for handler in self.handlers.iter().rev() {
            next.clear();
            handler.decompress(&current, &mut next)?;
            std::mem::swap(&mut current, &mut next);
        }

        Ok(current)
    }
}

impl Default for ByteHandlerPipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_roundtrip() {
        let handler = Lz4Handler::new();
        let input = b"hello world hello world hello world hello world";

        let mut compressed = Vec::new();
        handler.compress(input, &mut compressed).unwrap();

        let mut decompressed = Vec::new();
        handler.decompress(&compressed, &mut decompressed).unwrap();

        assert_eq!(decompressed, input);
    }

    #[test]
    fn test_identity_roundtrip() {
        let handler = IdentityHandler;
        let input = b"test data";

        let mut compressed = Vec::new();
        handler.compress(input, &mut compressed).unwrap();
        assert_eq!(compressed, input);

        let mut decompressed = Vec::new();
        handler.decompress(&compressed, &mut decompressed).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn test_pipeline_lz4() {
        let pipeline = ByteHandlerPipeline::lz4();
        let input = b"repeating data repeating data repeating data repeating data";

        let compressed = pipeline.compress(input).unwrap();
        assert!(compressed.len() < input.len()); // should actually compress

        let decompressed = pipeline.decompress(&compressed).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn test_empty_pipeline() {
        let pipeline = ByteHandlerPipeline::new();
        assert!(pipeline.is_empty());

        let input = b"test";
        let compressed = pipeline.compress(input).unwrap();
        assert_eq!(compressed, input);
    }

    #[test]
    fn test_lz4_empty_input() {
        let handler = Lz4Handler::new();
        let mut compressed = Vec::new();
        handler.compress(b"", &mut compressed).unwrap();

        let mut decompressed = Vec::new();
        handler.decompress(&compressed, &mut decompressed).unwrap();
        assert!(decompressed.is_empty());
    }
}
