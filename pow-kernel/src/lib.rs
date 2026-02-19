#[path = "../../src/backend/cpu/fixed_argon.rs"]
mod fixed_argon_impl;

pub use fixed_argon_impl::{Error, FixedArgon2id, PowBlock, Result};

#[derive(Debug, Clone)]
pub struct ReusablePowContext {
    hasher: FixedArgon2id,
    memory_blocks: Vec<PowBlock>,
}

impl ReusablePowContext {
    pub fn new(memory_kib: u32) -> Self {
        let hasher = FixedArgon2id::new(memory_kib);
        let memory_blocks = vec![PowBlock::default(); hasher.block_count()];
        Self {
            hasher,
            memory_blocks,
        }
    }

    pub fn block_count(&self) -> usize {
        self.memory_blocks.len()
    }

    pub fn hash_password_into(&mut self, pwd: &[u8], salt: &[u8], out: &mut [u8]) -> Result<()> {
        self.hasher
            .hash_password_into_with_memory(pwd, salt, out, &mut self.memory_blocks)
    }
}
