//! Page checksum utilities.
//!
//! Uses CRC32C for fast checksumming with hardware acceleration on modern CPUs.

/// Computes a CRC32 checksum for the given data.
///
/// # Example
///
/// ```rust
/// use nexus_storage::page::compute_checksum;
///
/// let data = b"Hello, NexusDB!";
/// let checksum = compute_checksum(data);
/// assert_ne!(checksum, 0);
/// ```
#[inline]
pub fn compute_checksum(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Verifies that the checksum matches the data.
///
/// # Example
///
/// ```rust
/// use nexus_storage::page::{compute_checksum, verify_checksum};
///
/// let data = b"Hello, NexusDB!";
/// let checksum = compute_checksum(data);
/// assert!(verify_checksum(data, checksum));
/// assert!(!verify_checksum(data, checksum + 1));
/// ```
#[inline]
pub fn verify_checksum(data: &[u8], expected: u32) -> bool {
    compute_checksum(data) == expected
}

/// Computes a checksum for a page, excluding the checksum field itself.
///
/// This is used when writing pages - we compute the checksum of all data
/// except the checksum field, then store it in the checksum field.
///
/// # Arguments
///
/// * `page_data` - The full page data
/// * `checksum_offset` - The offset of the checksum field (4 bytes)
///
/// # Example
///
/// ```rust
/// use nexus_storage::page::compute_page_checksum;
///
/// let mut page = vec![0u8; 8192];
/// // Checksum is at offset 20 in the header
/// let checksum = compute_page_checksum(&page, 20);
/// ```
pub fn compute_page_checksum(page_data: &[u8], checksum_offset: usize) -> u32 {
    debug_assert!(checksum_offset + 4 <= page_data.len());

    // Compute checksum of data before and after the checksum field
    let mut hasher = crc32fast::Hasher::new();

    // Hash everything before the checksum field
    if checksum_offset > 0 {
        hasher.update(&page_data[..checksum_offset]);
    }

    // Skip the 4-byte checksum field and hash the rest
    let after_checksum = checksum_offset + 4;
    if after_checksum < page_data.len() {
        hasher.update(&page_data[after_checksum..]);
    }

    hasher.finalize()
}

/// Represents a checksum verification result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ChecksumResult {
    /// Checksum is valid.
    Valid,
    /// Checksum is invalid (data corruption detected).
    Invalid {
        /// The expected checksum (from page header).
        expected: u32,
        /// The computed checksum.
        computed: u32,
    },
}

impl ChecksumResult {
    /// Returns true if the checksum is valid.
    #[inline]
    #[must_use]
    #[allow(dead_code)]
    pub const fn is_valid(self) -> bool {
        matches!(self, Self::Valid)
    }

    /// Returns true if the checksum is invalid.
    #[inline]
    #[must_use]
    #[allow(dead_code)]
    pub const fn is_invalid(self) -> bool {
        matches!(self, Self::Invalid { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_checksum() {
        let data = b"Hello, NexusDB!";
        let checksum = compute_checksum(data);

        // Should be deterministic
        assert_eq!(compute_checksum(data), checksum);

        // Different data should have different checksum
        let other_data = b"Hello, World!";
        assert_ne!(compute_checksum(other_data), checksum);
    }

    #[test]
    fn test_verify_checksum() {
        let data = b"Test data";
        let checksum = compute_checksum(data);

        assert!(verify_checksum(data, checksum));
        assert!(!verify_checksum(data, checksum + 1));
        assert!(!verify_checksum(data, 0));
    }

    #[test]
    fn test_empty_data() {
        let empty: &[u8] = &[];
        let checksum = compute_checksum(empty);
        assert!(verify_checksum(empty, checksum));
    }

    #[test]
    fn test_compute_page_checksum() {
        let mut page = vec![0u8; 8192];

        // Write some data
        page[0..4].copy_from_slice(&[0x4E, 0x58, 0x00, 0x00]); // Magic
        page[24..28].copy_from_slice(&[0xFF; 4]); // Placeholder for checksum at offset 24
        page[100..110].copy_from_slice(b"test data!");

        let checksum1 = compute_page_checksum(&page, 24);

        // Changing checksum field shouldn't affect the computed checksum
        page[24..28].copy_from_slice(&[0x00; 4]);
        let checksum2 = compute_page_checksum(&page, 24);

        assert_eq!(checksum1, checksum2);

        // But changing other data should
        page[100] = 0xFF;
        let checksum3 = compute_page_checksum(&page, 24);
        assert_ne!(checksum1, checksum3);
    }

    #[test]
    fn test_checksum_result() {
        assert!(ChecksumResult::Valid.is_valid());
        assert!(!ChecksumResult::Valid.is_invalid());

        let invalid = ChecksumResult::Invalid {
            expected: 100,
            computed: 200,
        };
        assert!(!invalid.is_valid());
        assert!(invalid.is_invalid());
    }
}
