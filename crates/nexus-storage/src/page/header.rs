//! Page header format.
//!
//! Every page in NexusDB starts with a 32-byte header containing metadata.
//!
//! # Header Layout (32 bytes)
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//!   0       2   magic (0x4E58 = "NX")
//!   2       1   page_type
//!   3       1   reserved
//!   4       8   page_id
//!  12       8   lsn (Log Sequence Number)
//!  20       4   checksum (CRC32C of page data, excluding this field)
//!  24       2   flags
//!  26       2   slot_count (number of records/entries)
//!  28       2   free_space_offset (start of free space)
//!  30       2   free_space_end (end of free space / start of data)
//! ```

use nexus_common::types::{Lsn, PageId};

use super::checksum::compute_page_checksum;
use super::types::{PageFlags, PageType};

/// Size of the page header in bytes.
pub const PAGE_HEADER_SIZE: usize = 32;

/// Offset of the checksum field in the header.
const CHECKSUM_OFFSET: usize = 20;

/// Page header structure.
///
/// This is a view into the first 32 bytes of a page. It provides
/// methods to read and write header fields safely.
///
/// # Example
///
/// ```rust
/// use nexus_storage::page::{PageHeader, PAGE_HEADER_SIZE};
/// use nexus_common::types::PageId;
///
/// let mut buffer = vec![0u8; 8192];
/// let mut header = PageHeader::new(&mut buffer);
///
/// // Initialize a new page
/// header.initialize(PageId::new(1));
/// assert_eq!(header.page_id(), PageId::new(1));
/// ```
#[derive(Debug)]
pub struct PageHeader<'a> {
    data: &'a mut [u8],
}

impl<'a> PageHeader<'a> {
    /// Creates a new PageHeader view into the given buffer.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is smaller than PAGE_HEADER_SIZE.
    #[inline]
    pub fn new(data: &'a mut [u8]) -> Self {
        assert!(
            data.len() >= PAGE_HEADER_SIZE,
            "buffer too small for page header"
        );
        Self { data }
    }

    /// Initializes a new page with default values.
    pub fn initialize(&mut self, page_id: PageId) {
        self.set_magic();
        self.set_page_type(PageType::Free);
        self.set_page_id(page_id);
        self.set_lsn(Lsn::INVALID);
        self.set_flags(PageFlags::empty());
        self.set_slot_count(0);
        // Free space starts right after header
        self.set_free_space_offset(PAGE_HEADER_SIZE as u16);
        // Data grows from end of page
        self.set_free_space_end(self.data.len() as u16);
    }

    /// Initializes a page with a specific type.
    pub fn initialize_with_type(&mut self, page_id: PageId, page_type: PageType) {
        self.initialize(page_id);
        self.set_page_type(page_type);
    }

    // =========================================================================
    // Magic (offset 0, 2 bytes)
    // =========================================================================

    /// Returns the magic bytes.
    #[inline]
    pub fn magic(&self) -> u16 {
        u16::from_le_bytes([self.data[0], self.data[1]])
    }

    /// Sets the magic bytes.
    #[inline]
    pub fn set_magic(&mut self) {
        let magic = super::PAGE_MAGIC;
        self.data[0..2].copy_from_slice(&magic.to_le_bytes());
    }

    /// Validates the magic bytes.
    #[inline]
    pub fn is_valid_magic(&self) -> bool {
        self.magic() == super::PAGE_MAGIC
    }

    // =========================================================================
    // Page Type (offset 2, 1 byte)
    // =========================================================================

    /// Returns the page type.
    #[inline]
    pub fn page_type(&self) -> PageType {
        PageType::from_u8(self.data[2]).unwrap_or(PageType::Free)
    }

    /// Sets the page type.
    #[inline]
    pub fn set_page_type(&mut self, page_type: PageType) {
        self.data[2] = page_type as u8;
    }

    // =========================================================================
    // Page ID (offset 4, 8 bytes)
    // =========================================================================

    /// Returns the page ID.
    #[inline]
    pub fn page_id(&self) -> PageId {
        let bytes: [u8; 8] = self.data[4..12].try_into().unwrap();
        PageId::new(u64::from_le_bytes(bytes))
    }

    /// Sets the page ID.
    #[inline]
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.data[4..12].copy_from_slice(&page_id.as_u64().to_le_bytes());
    }

    // =========================================================================
    // LSN (offset 12, 8 bytes)
    // =========================================================================

    /// Returns the LSN (Log Sequence Number).
    #[inline]
    pub fn lsn(&self) -> Lsn {
        let bytes: [u8; 8] = self.data[12..20].try_into().unwrap();
        Lsn::new(u64::from_le_bytes(bytes))
    }

    /// Sets the LSN.
    #[inline]
    pub fn set_lsn(&mut self, lsn: Lsn) {
        self.data[12..20].copy_from_slice(&lsn.as_u64().to_le_bytes());
    }

    // =========================================================================
    // Checksum (offset 20, 4 bytes)
    // =========================================================================

    /// Returns the stored checksum.
    #[inline]
    pub fn checksum(&self) -> u32 {
        let bytes: [u8; 4] = self.data[20..24].try_into().unwrap();
        u32::from_le_bytes(bytes)
    }

    /// Sets the checksum.
    #[inline]
    pub fn set_checksum(&mut self, checksum: u32) {
        self.data[20..24].copy_from_slice(&checksum.to_le_bytes());
    }

    /// Computes and stores the page checksum.
    ///
    /// Call this after all other modifications to the page are complete.
    pub fn update_checksum(&mut self) {
        let checksum = compute_page_checksum(self.data, CHECKSUM_OFFSET);
        self.set_checksum(checksum);
    }

    /// Verifies the page checksum.
    pub fn verify_checksum(&self) -> bool {
        let stored = self.checksum();
        let computed = compute_page_checksum(self.data, CHECKSUM_OFFSET);
        stored == computed
    }

    // =========================================================================
    // Flags (offset 24, 2 bytes)
    // =========================================================================

    /// Returns the page flags.
    #[inline]
    pub fn flags(&self) -> PageFlags {
        let bytes: [u8; 2] = self.data[24..26].try_into().unwrap();
        PageFlags::from_bits(u16::from_le_bytes(bytes))
    }

    /// Sets the page flags.
    #[inline]
    pub fn set_flags(&mut self, flags: PageFlags) {
        self.data[24..26].copy_from_slice(&flags.bits().to_le_bytes());
    }

    /// Sets a specific flag.
    #[inline]
    pub fn set_flag(&mut self, flag: u16) {
        let mut flags = self.flags();
        flags.set(flag);
        self.set_flags(flags);
    }

    /// Clears a specific flag.
    #[inline]
    pub fn clear_flag(&mut self, flag: u16) {
        let mut flags = self.flags();
        flags.clear(flag);
        self.set_flags(flags);
    }

    // =========================================================================
    // Slot Count (offset 26, 2 bytes)
    // =========================================================================

    /// Returns the number of slots/records in the page.
    #[inline]
    pub fn slot_count(&self) -> u16 {
        let bytes: [u8; 2] = self.data[26..28].try_into().unwrap();
        u16::from_le_bytes(bytes)
    }

    /// Sets the slot count.
    #[inline]
    pub fn set_slot_count(&mut self, count: u16) {
        self.data[26..28].copy_from_slice(&count.to_le_bytes());
    }

    /// Increments the slot count.
    #[inline]
    pub fn increment_slot_count(&mut self) {
        self.set_slot_count(self.slot_count() + 1);
    }

    /// Decrements the slot count.
    #[inline]
    pub fn decrement_slot_count(&mut self) {
        let count = self.slot_count();
        if count > 0 {
            self.set_slot_count(count - 1);
        }
    }

    // =========================================================================
    // Free Space Offset (offset 28, 2 bytes)
    // =========================================================================

    /// Returns the offset where free space begins (end of slot array).
    #[inline]
    pub fn free_space_offset(&self) -> u16 {
        let bytes: [u8; 2] = self.data[28..30].try_into().unwrap();
        u16::from_le_bytes(bytes)
    }

    /// Sets the free space offset.
    #[inline]
    pub fn set_free_space_offset(&mut self, offset: u16) {
        self.data[28..30].copy_from_slice(&offset.to_le_bytes());
    }

    // =========================================================================
    // Free Space End (offset 30, 2 bytes)
    // =========================================================================

    /// Returns the offset where data begins (end of free space).
    #[inline]
    pub fn free_space_end(&self) -> u16 {
        let bytes: [u8; 2] = self.data[30..32].try_into().unwrap();
        u16::from_le_bytes(bytes)
    }

    /// Sets the free space end offset.
    #[inline]
    pub fn set_free_space_end(&mut self, offset: u16) {
        self.data[30..32].copy_from_slice(&offset.to_le_bytes());
    }

    // =========================================================================
    // Computed Properties
    // =========================================================================

    /// Returns the amount of free space in the page.
    #[inline]
    pub fn free_space(&self) -> usize {
        let start = self.free_space_offset() as usize;
        let end = self.free_space_end() as usize;
        if end > start {
            end - start
        } else {
            0
        }
    }

    /// Returns true if the page has enough space for an allocation.
    #[inline]
    pub fn has_space_for(&self, slot_size: usize, record_size: usize) -> bool {
        self.free_space() >= slot_size + record_size
    }

    /// Returns a read-only view of the header bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[..PAGE_HEADER_SIZE]
    }
}

/// Read-only page header view.
#[derive(Debug)]
#[allow(dead_code)]
pub struct PageHeaderRef<'a> {
    data: &'a [u8],
}

#[allow(dead_code)]
impl<'a> PageHeaderRef<'a> {
    /// Creates a new read-only PageHeader view.
    #[inline]
    pub fn new(data: &'a [u8]) -> Self {
        assert!(
            data.len() >= PAGE_HEADER_SIZE,
            "buffer too small for page header"
        );
        Self { data }
    }

    /// Returns the magic bytes.
    #[inline]
    pub fn magic(&self) -> u16 {
        u16::from_le_bytes([self.data[0], self.data[1]])
    }

    /// Validates the magic bytes.
    #[inline]
    pub fn is_valid_magic(&self) -> bool {
        self.magic() == super::PAGE_MAGIC
    }

    /// Returns the page type.
    #[inline]
    pub fn page_type(&self) -> PageType {
        PageType::from_u8(self.data[2]).unwrap_or(PageType::Free)
    }

    /// Returns the page ID.
    #[inline]
    pub fn page_id(&self) -> PageId {
        let bytes: [u8; 8] = self.data[4..12].try_into().unwrap();
        PageId::new(u64::from_le_bytes(bytes))
    }

    /// Returns the LSN.
    #[inline]
    pub fn lsn(&self) -> Lsn {
        let bytes: [u8; 8] = self.data[12..20].try_into().unwrap();
        Lsn::new(u64::from_le_bytes(bytes))
    }

    /// Returns the checksum.
    #[inline]
    pub fn checksum(&self) -> u32 {
        let bytes: [u8; 4] = self.data[20..24].try_into().unwrap();
        u32::from_le_bytes(bytes)
    }

    /// Returns the page flags.
    #[inline]
    pub fn flags(&self) -> PageFlags {
        let bytes: [u8; 2] = self.data[24..26].try_into().unwrap();
        PageFlags::from_bits(u16::from_le_bytes(bytes))
    }

    /// Returns the slot count.
    #[inline]
    pub fn slot_count(&self) -> u16 {
        let bytes: [u8; 2] = self.data[26..28].try_into().unwrap();
        u16::from_le_bytes(bytes)
    }

    /// Returns the free space offset.
    #[inline]
    pub fn free_space_offset(&self) -> u16 {
        let bytes: [u8; 2] = self.data[28..30].try_into().unwrap();
        u16::from_le_bytes(bytes)
    }

    /// Returns the free space end.
    #[inline]
    pub fn free_space_end(&self) -> u16 {
        let bytes: [u8; 2] = self.data[30..32].try_into().unwrap();
        u16::from_le_bytes(bytes)
    }

    /// Returns the amount of free space.
    #[inline]
    pub fn free_space(&self) -> usize {
        let start = self.free_space_offset() as usize;
        let end = self.free_space_end() as usize;
        if end > start {
            end - start
        } else {
            0
        }
    }

    /// Verifies the page checksum.
    pub fn verify_checksum(&self) -> bool {
        let stored = self.checksum();
        let computed = compute_page_checksum(self.data, CHECKSUM_OFFSET);
        stored == computed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PAGE_SIZE: usize = 8192;

    fn create_test_page() -> Vec<u8> {
        vec![0u8; TEST_PAGE_SIZE]
    }

    #[test]
    fn test_header_size() {
        assert_eq!(PAGE_HEADER_SIZE, 32);
    }

    #[test]
    fn test_initialize() {
        let mut buffer = create_test_page();
        let mut header = PageHeader::new(&mut buffer);

        header.initialize(PageId::new(42));

        assert!(header.is_valid_magic());
        assert_eq!(header.page_id(), PageId::new(42));
        assert_eq!(header.page_type(), PageType::Free);
        assert_eq!(header.lsn(), Lsn::INVALID);
        assert_eq!(header.slot_count(), 0);
        assert_eq!(header.free_space_offset(), PAGE_HEADER_SIZE as u16);
        assert_eq!(header.free_space_end(), TEST_PAGE_SIZE as u16);
    }

    #[test]
    fn test_initialize_with_type() {
        let mut buffer = create_test_page();
        let mut header = PageHeader::new(&mut buffer);

        header.initialize_with_type(PageId::new(1), PageType::Leaf);

        assert_eq!(header.page_type(), PageType::Leaf);
    }

    #[test]
    fn test_page_type() {
        let mut buffer = create_test_page();
        let mut header = PageHeader::new(&mut buffer);

        header.set_page_type(PageType::Data);
        assert_eq!(header.page_type(), PageType::Data);

        header.set_page_type(PageType::Internal);
        assert_eq!(header.page_type(), PageType::Internal);
    }

    #[test]
    fn test_lsn() {
        let mut buffer = create_test_page();
        let mut header = PageHeader::new(&mut buffer);

        header.set_lsn(Lsn::new(12345));
        assert_eq!(header.lsn(), Lsn::new(12345));
    }

    #[test]
    fn test_flags() {
        let mut buffer = create_test_page();
        let mut header = PageHeader::new(&mut buffer);

        header.set_flag(PageFlags::DIRTY);
        assert!(header.flags().is_dirty());

        header.set_flag(PageFlags::PINNED);
        assert!(header.flags().is_dirty());
        assert!(header.flags().is_pinned());

        header.clear_flag(PageFlags::DIRTY);
        assert!(!header.flags().is_dirty());
        assert!(header.flags().is_pinned());
    }

    #[test]
    fn test_slot_count() {
        let mut buffer = create_test_page();
        let mut header = PageHeader::new(&mut buffer);

        assert_eq!(header.slot_count(), 0);

        header.increment_slot_count();
        assert_eq!(header.slot_count(), 1);

        header.increment_slot_count();
        assert_eq!(header.slot_count(), 2);

        header.decrement_slot_count();
        assert_eq!(header.slot_count(), 1);

        header.set_slot_count(100);
        assert_eq!(header.slot_count(), 100);
    }

    #[test]
    fn test_free_space() {
        let mut buffer = create_test_page();
        let mut header = PageHeader::new(&mut buffer);

        header.initialize(PageId::new(1));

        let expected_free = TEST_PAGE_SIZE - PAGE_HEADER_SIZE;
        assert_eq!(header.free_space(), expected_free);

        // Simulate adding a record
        header.set_free_space_end(8000);
        assert_eq!(header.free_space(), 8000 - PAGE_HEADER_SIZE);

        // Simulate adding a slot
        header.set_free_space_offset(36);
        assert_eq!(header.free_space(), 8000 - 36);
    }

    #[test]
    fn test_has_space_for() {
        let mut buffer = create_test_page();
        let mut header = PageHeader::new(&mut buffer);

        header.initialize(PageId::new(1));

        // Should have space for small allocations
        assert!(header.has_space_for(4, 100));

        // Should not have space for huge allocations
        assert!(!header.has_space_for(4, 10000));
    }

    #[test]
    fn test_checksum() {
        let mut buffer = create_test_page();
        let mut header = PageHeader::new(&mut buffer);

        header.initialize(PageId::new(1));
        header.update_checksum();

        assert!(header.verify_checksum());

        // Corrupt the page
        buffer[100] = 0xFF;
        let header = PageHeader::new(&mut buffer);
        assert!(!header.verify_checksum());
    }

    #[test]
    fn test_header_ref() {
        let mut buffer = create_test_page();
        {
            let mut header = PageHeader::new(&mut buffer);
            header.initialize(PageId::new(42));
            header.set_page_type(PageType::Leaf);
            header.set_slot_count(10);
        }

        let header_ref = PageHeaderRef::new(&buffer);
        assert!(header_ref.is_valid_magic());
        assert_eq!(header_ref.page_id(), PageId::new(42));
        assert_eq!(header_ref.page_type(), PageType::Leaf);
        assert_eq!(header_ref.slot_count(), 10);
    }

    #[test]
    #[should_panic(expected = "buffer too small")]
    fn test_header_buffer_too_small() {
        let mut buffer = vec![0u8; 16];
        let _header = PageHeader::new(&mut buffer);
    }
}
