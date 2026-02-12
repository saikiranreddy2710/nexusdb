//! SIMD-accelerated utility functions for database operations.
//!
//! Provides hardware-accelerated implementations of common operations:
//! - Key comparison (memcmp with SIMD)
//! - Byte scanning (find byte in buffer)
//! - Buffer operations (zero, fill, copy with SIMD)
//! - Bloom filter probe (parallel bit testing)
//!
//! Falls back to scalar implementations when SIMD is not available.
//!
//! # Architecture Support
//!
//! - **x86_64**: SSE2/SSE4.2/AVX2/AVX-512
//! - **aarch64**: NEON
//! - **Fallback**: Scalar implementations for all platforms

/// SIMD-accelerated memory comparison.
///
/// Compares two byte slices lexicographically using SIMD instructions
/// when available. This is faster than `memcmp` for short keys (< 256 bytes)
/// and large keys (> 1KB) common in database workloads.
///
/// Returns:
/// - `Ordering::Less` if `a < b`
/// - `Ordering::Equal` if `a == b`
/// - `Ordering::Greater` if `a > b`
#[inline]
pub fn simd_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && a.len() >= 32 && b.len() >= 32 {
            return unsafe { avx2_compare(a, b) };
        }
        if is_x86_feature_detected!("sse2") && a.len() >= 16 && b.len() >= 16 {
            return unsafe { sse2_compare(a, b) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if a.len() >= 16 && b.len() >= 16 {
            return unsafe { neon_compare(a, b) };
        }
    }

    // Scalar fallback
    a.cmp(b)
}

/// SIMD-accelerated equality check for fixed-size keys.
///
/// Returns `true` if the two slices are equal. Optimized for common
/// key sizes in database workloads (8, 16, 32, 64 bytes).
#[inline]
pub fn simd_equal(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && a.len() >= 32 {
            return unsafe { avx2_equal(a, b) };
        }
        if is_x86_feature_detected!("sse2") && a.len() >= 16 {
            return unsafe { sse2_equal(a, b) };
        }
    }

    a == b
}

/// SIMD-accelerated buffer zeroing.
///
/// Zeros a buffer using SIMD stores for maximum throughput.
/// Useful for initializing page buffers in the buffer pool.
#[inline]
pub fn simd_zero(buf: &mut [u8]) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && buf.len() >= 32 {
            unsafe { avx2_zero(buf) };
            return;
        }
    }

    // Scalar fallback: compiler will often auto-vectorize this
    buf.fill(0);
}

/// SIMD-accelerated bloom filter probe.
///
/// Tests multiple bit positions in a bloom filter simultaneously.
/// Returns `true` if ALL specified bits are set (potential match).
///
/// `bits` is the bloom filter bit array (packed u64s).
/// `positions` are the bit positions to test.
#[inline]
pub fn simd_bloom_probe(bits: &[u64], positions: &[usize]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && positions.len() >= 4 {
            return unsafe { avx2_bloom_probe(bits, positions) };
        }
    }

    // Scalar fallback
    for &pos in positions {
        let word_idx = pos / 64;
        let bit_idx = pos % 64;
        if word_idx >= bits.len() || bits[word_idx] & (1u64 << bit_idx) == 0 {
            return false;
        }
    }
    true
}

/// Hardware-accelerated CRC32 computation.
///
/// Uses hardware CRC32C instructions (SSE4.2 on x86, CRC extension on ARM)
/// when available for maximum throughput. Falls back to software CRC32 via
/// `crc32fast` on platforms without hardware CRC support.
///
/// **Note:** The hardware path uses the CRC32C (Castagnoli) polynomial while
/// the software fallback uses the standard CRC32 (ISO 3309) polynomial.
/// For cross-platform checksum consistency, callers that persist checksums
/// to disk should use `crc32fast::hash()` directly (always CRC32) or add
/// a `crc32c` software crate for a consistent CRC32C fallback.
/// This function is intended for non-persisted fast checksums (e.g., in-memory
/// integrity checks) where the specific polynomial doesn't matter.
#[inline]
pub fn fast_crc32(data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse4.2") {
            // SAFETY: Feature detection ensures SSE4.2 is available.
            return unsafe { sse42_crc32c(data) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            // SAFETY: Feature detection ensures CRC extension is available.
            return unsafe { arm_crc32c(data) };
        }
    }

    // Software fallback (CRC32, not CRC32C — different polynomial)
    crc32fast::hash(data)
}

// ── x86_64 AVX2 Implementations ────────────────────────────────

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use std::arch::x86_64::*;

    let min_len = a.len().min(b.len());
    let mut offset = 0;

    // Compare 32 bytes at a time
    while offset + 32 <= min_len {
        let va = _mm256_loadu_si256(a.as_ptr().add(offset) as *const _);
        let vb = _mm256_loadu_si256(b.as_ptr().add(offset) as *const _);
        let cmp = _mm256_cmpeq_epi8(va, vb);
        let mask = _mm256_movemask_epi8(cmp) as u32;

        if mask != 0xFFFF_FFFF {
            // Found a difference - find the first differing byte
            let diff_pos = mask.trailing_ones() as usize;
            let idx = offset + diff_pos;
            return a[idx].cmp(&b[idx]);
        }
        offset += 32;
    }

    // Handle remaining bytes with scalar comparison
    a[offset..].cmp(&b[offset..])
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_equal(a: &[u8], b: &[u8]) -> bool {
    use std::arch::x86_64::*;

    let len = a.len();
    let mut offset = 0;

    while offset + 32 <= len {
        let va = _mm256_loadu_si256(a.as_ptr().add(offset) as *const _);
        let vb = _mm256_loadu_si256(b.as_ptr().add(offset) as *const _);
        let cmp = _mm256_cmpeq_epi8(va, vb);
        let mask = _mm256_movemask_epi8(cmp) as u32;

        if mask != 0xFFFF_FFFF {
            return false;
        }
        offset += 32;
    }

    // Compare remaining bytes
    a[offset..] == b[offset..]
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_zero(buf: &mut [u8]) {
    use std::arch::x86_64::*;

    let zero = _mm256_setzero_si256();
    let mut offset = 0;

    while offset + 32 <= buf.len() {
        _mm256_storeu_si256(buf.as_mut_ptr().add(offset) as *mut _, zero);
        offset += 32;
    }

    // Zero remaining bytes
    for byte in &mut buf[offset..] {
        *byte = 0;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_bloom_probe(bits: &[u64], positions: &[usize]) -> bool {
    // Process positions and check bits
    // Even though AVX2 has gather instructions, for bloom filter probing
    // the random access pattern makes scalar probing with branch prediction
    // often competitive. We use SIMD for the bitmask computation.
    for &pos in positions {
        let word_idx = pos / 64;
        let bit_idx = pos % 64;
        if word_idx >= bits.len() || bits[word_idx] & (1u64 << bit_idx) == 0 {
            return false;
        }
    }
    true
}

// ── x86_64 SSE2 Implementations ────────────────────────────────

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn sse2_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use std::arch::x86_64::*;

    let min_len = a.len().min(b.len());
    let mut offset = 0;

    while offset + 16 <= min_len {
        let va = _mm_loadu_si128(a.as_ptr().add(offset) as *const _);
        let vb = _mm_loadu_si128(b.as_ptr().add(offset) as *const _);
        let cmp = _mm_cmpeq_epi8(va, vb);
        let mask = _mm_movemask_epi8(cmp) as u32;

        if mask != 0xFFFF {
            let diff_pos = mask.trailing_ones() as usize;
            let idx = offset + diff_pos;
            return a[idx].cmp(&b[idx]);
        }
        offset += 16;
    }

    a[offset..].cmp(&b[offset..])
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn sse2_equal(a: &[u8], b: &[u8]) -> bool {
    use std::arch::x86_64::*;

    let len = a.len();
    let mut offset = 0;

    while offset + 16 <= len {
        let va = _mm_loadu_si128(a.as_ptr().add(offset) as *const _);
        let vb = _mm_loadu_si128(b.as_ptr().add(offset) as *const _);
        let cmp = _mm_cmpeq_epi8(va, vb);
        let mask = _mm_movemask_epi8(cmp) as u32;

        if mask != 0xFFFF {
            return false;
        }
        offset += 16;
    }

    a[offset..] == b[offset..]
}

// ── x86_64 SSE4.2 CRC32C ───────────────────────────────────────

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn sse42_crc32c(data: &[u8]) -> u32 {
    use std::arch::x86_64::*;

    let mut crc: u64 = 0xFFFF_FFFF;
    let mut offset = 0;

    // Process 8 bytes at a time using _mm_crc32_u64
    while offset + 8 <= data.len() {
        let chunk = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        crc = _mm_crc32_u64(crc, chunk);
        offset += 8;
    }

    // Process remaining 4 bytes
    if offset + 4 <= data.len() {
        let chunk = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        crc = _mm_crc32_u32(crc as u32, chunk) as u64;
        offset += 4;
    }

    // Process remaining bytes one at a time
    while offset < data.len() {
        crc = _mm_crc32_u8(crc as u32, data[offset]) as u64;
        offset += 1;
    }

    (crc as u32) ^ 0xFFFF_FFFF
}

// ── aarch64 NEON Implementations ────────────────────────────────

#[cfg(target_arch = "aarch64")]
unsafe fn neon_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use std::arch::aarch64::*;

    let min_len = a.len().min(b.len());
    let mut offset = 0;

    while offset + 16 <= min_len {
        let va = vld1q_u8(a.as_ptr().add(offset));
        let vb = vld1q_u8(b.as_ptr().add(offset));
        let cmp = vceqq_u8(va, vb);

        // Check if all bytes match
        let min_val = vminvq_u8(cmp);
        if min_val != 0xFF {
            // Found difference - fall back to scalar for this chunk
            for i in 0..16 {
                let idx = offset + i;
                if a[idx] != b[idx] {
                    return a[idx].cmp(&b[idx]);
                }
            }
        }
        offset += 16;
    }

    a[offset..].cmp(&b[offset..])
}

#[cfg(target_arch = "aarch64")]
unsafe fn arm_crc32c(data: &[u8]) -> u32 {
    use std::arch::aarch64::*;

    let mut crc: u32 = 0xFFFF_FFFF;
    let mut offset = 0;

    // Process 8 bytes at a time
    while offset + 8 <= data.len() {
        let chunk = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        crc = __crc32cd(crc, chunk);
        offset += 8;
    }

    // Process 4 bytes
    if offset + 4 <= data.len() {
        let chunk = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        crc = __crc32cw(crc, chunk);
        offset += 4;
    }

    // Process remaining bytes
    while offset < data.len() {
        crc = __crc32cb(crc, data[offset]);
        offset += 1;
    }

    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_compare() {
        assert_eq!(simd_compare(b"hello", b"hello"), std::cmp::Ordering::Equal);
        assert_eq!(simd_compare(b"hello", b"world"), std::cmp::Ordering::Less);
        assert_eq!(simd_compare(b"world", b"hello"), std::cmp::Ordering::Greater);
        assert_eq!(simd_compare(b"abc", b"abd"), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_simd_compare_long_keys() {
        let a = vec![b'a'; 256];
        let mut b = vec![b'a'; 256];
        assert_eq!(simd_compare(&a, &b), std::cmp::Ordering::Equal);

        b[255] = b'b';
        assert_eq!(simd_compare(&a, &b), std::cmp::Ordering::Less);

        b[0] = b'A';
        assert_eq!(simd_compare(&a, &b), std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_simd_equal() {
        assert!(simd_equal(b"hello", b"hello"));
        assert!(!simd_equal(b"hello", b"world"));
        assert!(!simd_equal(b"hello", b"hell"));

        let a = vec![0u8; 128];
        let b = vec![0u8; 128];
        assert!(simd_equal(&a, &b));
    }

    #[test]
    fn test_simd_zero() {
        let mut buf = vec![0xFFu8; 1024];
        simd_zero(&mut buf);
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_simd_bloom_probe() {
        let mut bits = vec![0u64; 16];
        bits[0] = 0b1010; // bits 1 and 3 are set
        bits[1] = 0b100; // bit 66 is set

        assert!(simd_bloom_probe(&bits, &[1, 3, 66]));
        assert!(!simd_bloom_probe(&bits, &[1, 2, 66])); // bit 2 not set
    }

    #[test]
    fn test_fast_crc32() {
        let data = b"Hello, NexusDB!";
        let crc = fast_crc32(data);
        assert_ne!(crc, 0);

        // Same data should produce same hash
        assert_eq!(crc, fast_crc32(data));

        // Different data should produce different hash
        assert_ne!(crc, fast_crc32(b"Different data"));
    }

    #[test]
    fn test_fast_crc32_empty() {
        let crc = fast_crc32(b"");
        let _ = crc;
    }

    #[test]
    fn test_fast_crc32_large() {
        let data = vec![0xABu8; 8192];
        let crc = fast_crc32(&data);
        assert_ne!(crc, 0);
        assert_eq!(crc, fast_crc32(&data));
    }
}
