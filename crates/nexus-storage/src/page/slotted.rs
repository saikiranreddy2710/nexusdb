//! Slotted page format for variable-length records.
//!
//! A slotted page stores variable-length records efficiently using an
//! indirection layer (the slot array). This allows records to be:
//!
//! - Inserted without moving existing records
//! - Deleted by marking slots as empty
//! - Compacted to reclaim fragmented space
//!
//! # Page Layout
//!
//! ```text
//! +----------------------+
//! |    Page Header       |  32 bytes (see header.rs)
//! +----------------------+
//! |    Slot Array        |  4 bytes per slot, grows downward
//! |      [slot 0]        |  offset: 2 bytes, length: 2 bytes
//! |      [slot 1]        |
//! |      [slot 2]        |
//! |        ...           |
//! +----------------------+
//! |                      |
//! |    Free Space        |  unallocated space
//! |                      |
//! +----------------------+
//! |    Record Data       |  grows upward from end of page
//! |     [record 2]       |
//! |     [record 1]       |
//! |     [record 0]       |
//! +----------------------+
//! ```
//!
//! The slot array grows downward from the header, while record data
//! grows upward from the end of the page. This maximizes space usage.

use super::header::{PageHeader, PAGE_HEADER_SIZE};
use super::types::{PageFlags, PageType};
use nexus_common::types::PageId;

/// Size of each slot in bytes (offset: 2, length: 2).
pub const SLOT_SIZE: usize = 4;

/// Maximum record size that can fit in a single page.
pub const MAX_RECORD_SIZE: usize = super::PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE;

/// Marker for a deleted/empty slot.
const SLOT_DELETED: u16 = 0xFFFF;

/// A slot ID is an index into the slot array.
pub type SlotId = u16;

/// A slot in the slot array.
///
/// Each slot is 4 bytes: 2 bytes for offset, 2 bytes for length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slot {
    /// Offset from the start of the page to the record data.
    /// A value of 0xFFFF indicates a deleted slot.
    pub offset: u16,
    /// Length of the record in bytes.
    /// A value of 0 with offset != SLOT_DELETED indicates an empty record.
    pub length: u16,
}

impl Slot {
    /// Creates a new slot.
    #[inline]
    pub const fn new(offset: u16, length: u16) -> Self {
        Self { offset, length }
    }

    /// Creates a deleted/empty slot.
    #[inline]
    pub const fn deleted() -> Self {
        Self {
            offset: SLOT_DELETED,
            length: 0,
        }
    }

    /// Returns true if this slot is deleted.
    #[inline]
    pub const fn is_deleted(&self) -> bool {
        self.offset == SLOT_DELETED
    }

    /// Returns true if this slot is empty (has no data).
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.is_deleted() || self.length == 0
    }

    /// Reads a slot from bytes.
    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() >= SLOT_SIZE);
        Self {
            offset: u16::from_le_bytes([bytes[0], bytes[1]]),
            length: u16::from_le_bytes([bytes[2], bytes[3]]),
        }
    }

    /// Writes the slot to bytes.
    #[inline]
    pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
        let mut bytes = [0u8; SLOT_SIZE];
        bytes[0..2].copy_from_slice(&self.offset.to_le_bytes());
        bytes[2..4].copy_from_slice(&self.length.to_le_bytes());
        bytes
    }
}

impl Default for Slot {
    fn default() -> Self {
        Self::deleted()
    }
}

/// A reference to a record in a slotted page.
#[derive(Debug, Clone)]
pub struct RecordRef<'a> {
    /// The slot ID.
    pub slot_id: SlotId,
    /// The record data.
    pub data: &'a [u8],
}

/// A slotted page for storing variable-length records.
///
/// This provides a high-level interface for manipulating slotted pages.
///
/// # Example
///
/// ```rust
/// use nexus_storage::page::{SlottedPage, PageType};
/// use nexus_common::types::PageId;
///
/// let mut buffer = vec![0u8; 8192];
/// let mut page = SlottedPage::new(&mut buffer);
///
/// // Initialize as a leaf page
/// page.initialize(PageId::new(1), PageType::Leaf);
///
/// // Insert a record
/// let slot_id = page.insert_record(b"Hello, NexusDB!").unwrap();
/// assert_eq!(slot_id, 0);
///
/// // Read the record
/// let record = page.get_record(slot_id).unwrap();
/// assert_eq!(record, b"Hello, NexusDB!");
/// ```
pub struct SlottedPage<'a> {
    data: &'a mut [u8],
}

impl<'a> SlottedPage<'a> {
    /// Creates a new slotted page view into the given buffer.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is smaller than PAGE_HEADER_SIZE.
    #[inline]
    pub fn new(data: &'a mut [u8]) -> Self {
        assert!(
            data.len() >= PAGE_HEADER_SIZE,
            "buffer too small for slotted page"
        );
        Self { data }
    }

    /// Returns the page size.
    #[inline]
    pub fn page_size(&self) -> usize {
        self.data.len()
    }

    /// Returns a mutable reference to the page header.
    #[inline]
    pub fn header_mut(&mut self) -> PageHeader<'_> {
        PageHeader::new(self.data)
    }

    /// Initializes the page.
    pub fn initialize(&mut self, page_id: PageId, page_type: PageType) {
        let mut header = PageHeader::new(self.data);
        header.initialize_with_type(page_id, page_type);
    }

    /// Returns the number of slots in the page.
    #[inline]
    pub fn slot_count(&self) -> u16 {
        self.header_ref().slot_count()
    }

    /// Returns the free space offset (end of slot array).
    #[inline]
    fn free_space_offset(&self) -> usize {
        self.header_ref().free_space_offset() as usize
    }

    /// Returns the free space end (start of record data).
    #[inline]
    fn free_space_end(&self) -> usize {
        self.header_ref().free_space_end() as usize
    }

    /// Returns the amount of free space available.
    #[inline]
    pub fn free_space(&self) -> usize {
        let offset = self.free_space_offset();
        let end = self.free_space_end();
        if end > offset {
            end - offset
        } else {
            0
        }
    }

    /// Returns the usable free space after accounting for a new slot.
    #[inline]
    pub fn usable_free_space(&self) -> usize {
        let free = self.free_space();
        if free >= SLOT_SIZE {
            free - SLOT_SIZE
        } else {
            0
        }
    }

    /// Returns true if the page can fit a record of the given size.
    #[inline]
    pub fn can_fit(&self, record_size: usize) -> bool {
        self.free_space() >= SLOT_SIZE + record_size
    }

    /// Returns the offset in the buffer where a slot is stored.
    #[inline]
    fn slot_offset(&self, slot_id: SlotId) -> usize {
        PAGE_HEADER_SIZE + (slot_id as usize) * SLOT_SIZE
    }

    /// Reads a slot from the page.
    pub fn get_slot(&self, slot_id: SlotId) -> Option<Slot> {
        if slot_id >= self.slot_count() {
            return None;
        }

        let offset = self.slot_offset(slot_id);
        if offset + SLOT_SIZE > self.data.len() {
            return None;
        }

        Some(Slot::from_bytes(&self.data[offset..offset + SLOT_SIZE]))
    }

    /// Writes a slot to the page.
    fn set_slot(&mut self, slot_id: SlotId, slot: Slot) {
        let offset = self.slot_offset(slot_id);
        self.data[offset..offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());
    }

    /// Returns the record data for a slot.
    pub fn get_record(&self, slot_id: SlotId) -> Option<&[u8]> {
        let slot = self.get_slot(slot_id)?;

        if slot.is_deleted() {
            return None;
        }

        let offset = slot.offset as usize;
        let end = offset + slot.length as usize;

        if end > self.data.len() {
            return None;
        }

        Some(&self.data[offset..end])
    }

    /// Returns a mutable reference to the record data.
    pub fn get_record_mut(&mut self, slot_id: SlotId) -> Option<&mut [u8]> {
        let slot = self.get_slot(slot_id)?;

        if slot.is_deleted() {
            return None;
        }

        let offset = slot.offset as usize;
        let end = offset + slot.length as usize;

        if end > self.data.len() {
            return None;
        }

        Some(&mut self.data[offset..end])
    }

    /// Inserts a record into the page.
    ///
    /// Returns the slot ID of the inserted record, or `None` if there's no space.
    pub fn insert_record(&mut self, record: &[u8]) -> Option<SlotId> {
        let record_len = record.len();

        if record_len > MAX_RECORD_SIZE {
            return None;
        }

        if !self.can_fit(record_len) {
            return None;
        }

        // Find or create a slot
        let slot_id = self.find_or_create_slot()?;

        // Calculate where to put the record (grows upward from end)
        let record_offset = self.free_space_end() - record_len;

        // Copy record data
        self.data[record_offset..record_offset + record_len].copy_from_slice(record);

        // Update the slot
        self.set_slot(slot_id, Slot::new(record_offset as u16, record_len as u16));

        // Update header
        let mut header = self.header_mut();
        header.set_free_space_end(record_offset as u16);

        Some(slot_id)
    }

    /// Finds a deleted slot or creates a new one.
    fn find_or_create_slot(&mut self) -> Option<SlotId> {
        let slot_count = self.slot_count();

        // Look for a deleted slot to reuse
        for i in 0..slot_count {
            if let Some(slot) = self.get_slot(i) {
                if slot.is_deleted() {
                    return Some(i);
                }
            }
        }

        // Need to create a new slot
        let new_slot_offset = PAGE_HEADER_SIZE + (slot_count as usize) * SLOT_SIZE;

        // Check if we have space for the new slot
        if new_slot_offset + SLOT_SIZE > self.free_space_end() {
            return None;
        }

        // Update header
        let mut header = self.header_mut();
        header.increment_slot_count();
        header.set_free_space_offset(new_slot_offset as u16 + SLOT_SIZE as u16);

        Some(slot_count)
    }

    /// Deletes a record by marking its slot as deleted.
    ///
    /// Returns the size of the deleted record, or `None` if the slot doesn't exist.
    pub fn delete_record(&mut self, slot_id: SlotId) -> Option<usize> {
        let slot = self.get_slot(slot_id)?;

        if slot.is_deleted() {
            return None;
        }

        let size = slot.length as usize;

        // Mark the slot as deleted
        self.set_slot(slot_id, Slot::deleted());

        // Mark page as having tombstones
        let mut header = self.header_mut();
        header.set_flag(PageFlags::HAS_TOMBSTONES);

        Some(size)
    }

    /// Updates a record in place if the new data fits.
    ///
    /// Returns `true` if the update succeeded, `false` if the new record is too large.
    pub fn update_record_in_place(&mut self, slot_id: SlotId, new_data: &[u8]) -> bool {
        let slot = match self.get_slot(slot_id) {
            Some(s) if !s.is_deleted() => s,
            _ => return false,
        };

        // Can only update in place if new data fits in old slot
        if new_data.len() > slot.length as usize {
            return false;
        }

        let offset = slot.offset as usize;

        // Copy new data
        self.data[offset..offset + new_data.len()].copy_from_slice(new_data);

        // Update slot length if shrunk
        if new_data.len() < slot.length as usize {
            self.set_slot(slot_id, Slot::new(slot.offset, new_data.len() as u16));
        }

        true
    }

    /// Updates a record, potentially relocating it.
    ///
    /// If the new data doesn't fit in the old slot, the record is deleted
    /// and re-inserted. This may fail if the page is too full.
    pub fn update_record(&mut self, slot_id: SlotId, new_data: &[u8]) -> bool {
        // Try in-place update first
        if self.update_record_in_place(slot_id, new_data) {
            return true;
        }

        // Need to relocate - delete old and insert new
        let Some(slot) = self.get_slot(slot_id) else {
            return false;
        };

        if slot.is_deleted() {
            return false;
        }

        // Calculate if we have enough space
        // After deleting: current_free + old_record_size
        // Needed: new_record_size (slot already exists)
        let old_size = slot.length as usize;
        let needed_space = new_data.len();

        if self.free_space() + old_size < needed_space {
            return false;
        }

        // Delete old record (marks slot as deleted)
        self.delete_record(slot_id);

        // Calculate new offset
        let record_offset = self.free_space_end() - new_data.len();

        // Copy new data
        self.data[record_offset..record_offset + new_data.len()].copy_from_slice(new_data);

        // Update the slot (reuse the same slot ID)
        self.set_slot(
            slot_id,
            Slot::new(record_offset as u16, new_data.len() as u16),
        );

        // Update free space end
        self.header_mut().set_free_space_end(record_offset as u16);

        true
    }

    /// Returns an iterator over all non-deleted records.
    pub fn records(&self) -> impl Iterator<Item = RecordRef<'_>> {
        let slot_count = self.slot_count();

        (0..slot_count).filter_map(move |slot_id| {
            let slot = self.get_slot(slot_id)?;
            if slot.is_deleted() {
                return None;
            }

            let offset = slot.offset as usize;
            let end = offset + slot.length as usize;
            if end > self.data.len() {
                return None;
            }

            Some(RecordRef {
                slot_id,
                data: &self.data[offset..end],
            })
        })
    }

    /// Returns the number of non-deleted records.
    pub fn record_count(&self) -> usize {
        (0..self.slot_count())
            .filter(|&i| self.get_slot(i).map(|s| !s.is_deleted()).unwrap_or(false))
            .count()
    }

    /// Compacts the page by eliminating dead space.
    ///
    /// This moves all records to be contiguous and removes deleted slots.
    /// Returns the amount of space recovered.
    pub fn compact(&mut self) -> usize {
        let slot_count = self.slot_count();
        let page_size = self.page_size();
        let old_free_space = self.free_space();

        // Collect all non-deleted records
        let mut records: Vec<(SlotId, Vec<u8>)> = Vec::new();
        for slot_id in 0..slot_count {
            if let Some(slot) = self.get_slot(slot_id) {
                if !slot.is_deleted() {
                    let offset = slot.offset as usize;
                    let end = offset + slot.length as usize;
                    if end <= self.data.len() {
                        records.push((slot_id, self.data[offset..end].to_vec()));
                    }
                }
            }
        }

        // Re-initialize header, keeping the same page ID and type
        let _page_id = self.header_ref().page_id();
        let _page_type = self.header_ref().page_type();

        // Clear slot array and data area
        let new_slot_count = records.len() as u16;
        let new_free_space_offset = PAGE_HEADER_SIZE + (new_slot_count as usize) * SLOT_SIZE;

        {
            let mut header = self.header_mut();
            header.set_slot_count(new_slot_count);
            header.set_free_space_offset(new_free_space_offset as u16);
            header.set_free_space_end(page_size as u16);
            header.clear_flag(PageFlags::HAS_TOMBSTONES);
            header.clear_flag(PageFlags::NEEDS_COMPACTION);
        }

        // Re-insert records with new slot IDs
        let mut data_offset = page_size;
        for (new_slot_id, (_, record_data)) in records.iter().enumerate() {
            data_offset -= record_data.len();

            // Copy record data
            self.data[data_offset..data_offset + record_data.len()].copy_from_slice(record_data);

            // Update slot
            self.set_slot(
                new_slot_id as SlotId,
                Slot::new(data_offset as u16, record_data.len() as u16),
            );
        }

        // Update free space end
        self.header_mut().set_free_space_end(data_offset as u16);

        // Calculate recovered space
        self.free_space().saturating_sub(old_free_space)
    }

    /// Returns true if the page should be compacted.
    ///
    /// This is based on the ratio of dead space to total space.
    pub fn should_compact(&self) -> bool {
        let total_slots = self.slot_count() as usize;
        let live_slots = self.record_count();

        if total_slots == 0 {
            return false;
        }

        // Compact if more than 25% of slots are deleted
        let deleted_ratio = (total_slots - live_slots) as f64 / total_slots as f64;
        deleted_ratio > 0.25
    }

    /// Calculates the total size of all records in the page.
    pub fn total_record_size(&self) -> usize {
        (0..self.slot_count())
            .filter_map(|i| self.get_slot(i))
            .filter(|s| !s.is_deleted())
            .map(|s| s.length as usize)
            .sum()
    }

    /// Returns the fill ratio of the page (0.0 to 1.0).
    pub fn fill_ratio(&self) -> f64 {
        let usable_space = self.page_size() - PAGE_HEADER_SIZE;
        let used_space = self.total_record_size() + self.slot_count() as usize * SLOT_SIZE;
        used_space as f64 / usable_space as f64
    }

    /// Updates the page checksum.
    pub fn update_checksum(&mut self) {
        self.header_mut().update_checksum();
    }

    /// Verifies the page checksum.
    pub fn verify_checksum(&self) -> bool {
        self.header_ref().verify_checksum()
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    fn header_ref(&self) -> super::header::PageHeaderRef<'_> {
        super::header::PageHeaderRef::new(self.data)
    }
}

/// Read-only slotted page view.
#[allow(dead_code)]
pub struct SlottedPageRef<'a> {
    data: &'a [u8],
}

#[allow(dead_code)]
impl<'a> SlottedPageRef<'a> {
    /// Creates a new read-only slotted page view.
    #[inline]
    pub fn new(data: &'a [u8]) -> Self {
        assert!(
            data.len() >= PAGE_HEADER_SIZE,
            "buffer too small for slotted page"
        );
        Self { data }
    }

    /// Returns the page size.
    #[inline]
    pub fn page_size(&self) -> usize {
        self.data.len()
    }

    /// Returns the number of slots.
    #[inline]
    pub fn slot_count(&self) -> u16 {
        let bytes: [u8; 2] = self.data[26..28].try_into().unwrap();
        u16::from_le_bytes(bytes)
    }

    /// Returns the offset where a slot is stored.
    #[inline]
    fn slot_offset(&self, slot_id: SlotId) -> usize {
        PAGE_HEADER_SIZE + (slot_id as usize) * SLOT_SIZE
    }

    /// Reads a slot.
    pub fn get_slot(&self, slot_id: SlotId) -> Option<Slot> {
        if slot_id >= self.slot_count() {
            return None;
        }

        let offset = self.slot_offset(slot_id);
        if offset + SLOT_SIZE > self.data.len() {
            return None;
        }

        Some(Slot::from_bytes(&self.data[offset..offset + SLOT_SIZE]))
    }

    /// Returns the record data for a slot.
    pub fn get_record(&self, slot_id: SlotId) -> Option<&[u8]> {
        let slot = self.get_slot(slot_id)?;

        if slot.is_deleted() {
            return None;
        }

        let offset = slot.offset as usize;
        let end = offset + slot.length as usize;

        if end > self.data.len() {
            return None;
        }

        Some(&self.data[offset..end])
    }

    /// Returns an iterator over all non-deleted records.
    pub fn records(&self) -> impl Iterator<Item = RecordRef<'_>> {
        let slot_count = self.slot_count();

        (0..slot_count).filter_map(move |slot_id| {
            let slot = self.get_slot(slot_id)?;
            if slot.is_deleted() {
                return None;
            }

            let offset = slot.offset as usize;
            let end = offset + slot.length as usize;
            if end > self.data.len() {
                return None;
            }

            Some(RecordRef {
                slot_id,
                data: &self.data[offset..end],
            })
        })
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
    fn test_slot() {
        let slot = Slot::new(100, 50);
        assert_eq!(slot.offset, 100);
        assert_eq!(slot.length, 50);
        assert!(!slot.is_deleted());
        assert!(!slot.is_empty());

        let deleted = Slot::deleted();
        assert!(deleted.is_deleted());
        assert!(deleted.is_empty());
    }

    #[test]
    fn test_slot_serialization() {
        let slot = Slot::new(0x1234, 0x5678);
        let bytes = slot.to_bytes();
        let recovered = Slot::from_bytes(&bytes);
        assert_eq!(slot, recovered);
    }

    #[test]
    fn test_initialize() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(42), PageType::Leaf);

        assert_eq!(page.slot_count(), 0);
        assert!(page.free_space() > 0);
    }

    #[test]
    fn test_insert_record() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        let slot_id = page.insert_record(b"Hello, World!").unwrap();
        assert_eq!(slot_id, 0);

        let record = page.get_record(slot_id).unwrap();
        assert_eq!(record, b"Hello, World!");
    }

    #[test]
    fn test_multiple_inserts() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        let slot0 = page.insert_record(b"Record 0").unwrap();
        let slot1 = page.insert_record(b"Record 1").unwrap();
        let slot2 = page.insert_record(b"Record 2").unwrap();

        assert_eq!(slot0, 0);
        assert_eq!(slot1, 1);
        assert_eq!(slot2, 2);

        assert_eq!(page.get_record(slot0).unwrap(), b"Record 0");
        assert_eq!(page.get_record(slot1).unwrap(), b"Record 1");
        assert_eq!(page.get_record(slot2).unwrap(), b"Record 2");

        assert_eq!(page.slot_count(), 3);
        assert_eq!(page.record_count(), 3);
    }

    #[test]
    fn test_delete_record() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        let slot0 = page.insert_record(b"Record 0").unwrap();
        let slot1 = page.insert_record(b"Record 1").unwrap();

        let deleted_size = page.delete_record(slot0).unwrap();
        assert_eq!(deleted_size, 8);

        assert!(page.get_record(slot0).is_none());
        assert!(page.get_record(slot1).is_some());

        assert_eq!(page.slot_count(), 2);
        assert_eq!(page.record_count(), 1);
    }

    #[test]
    fn test_slot_reuse() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        let slot0 = page.insert_record(b"Record 0").unwrap();
        let _slot1 = page.insert_record(b"Record 1").unwrap();

        // Delete slot 0
        page.delete_record(slot0);

        // Insert new record - should reuse slot 0
        let slot_new = page.insert_record(b"New record").unwrap();
        assert_eq!(slot_new, 0);

        assert_eq!(page.get_record(slot_new).unwrap(), b"New record");
    }

    #[test]
    fn test_update_in_place() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        let slot = page.insert_record(b"Hello, World!").unwrap();

        // Update with smaller data
        assert!(page.update_record_in_place(slot, b"Hi!"));
        assert_eq!(page.get_record(slot).unwrap(), b"Hi!");

        // Update with larger data should fail
        assert!(!page.update_record_in_place(slot, b"This is much longer"));
    }

    #[test]
    fn test_update_record() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        let slot = page.insert_record(b"Short").unwrap();

        // Update with larger data (should relocate)
        assert!(page.update_record(slot, b"This is a much longer record"));
        assert_eq!(
            page.get_record(slot).unwrap(),
            b"This is a much longer record"
        );
    }

    #[test]
    fn test_records_iterator() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        page.insert_record(b"A").unwrap();
        page.insert_record(b"B").unwrap();
        page.insert_record(b"C").unwrap();

        let records: Vec<_> = page.records().collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].data, b"A");
        assert_eq!(records[1].data, b"B");
        assert_eq!(records[2].data, b"C");
    }

    #[test]
    fn test_compact() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        // Insert some records
        let slot0 = page.insert_record(b"Record 0").unwrap();
        let slot1 = page.insert_record(b"Record 1").unwrap();
        let slot2 = page.insert_record(b"Record 2").unwrap();

        // Delete middle record
        page.delete_record(slot1);

        assert_eq!(page.slot_count(), 3);
        assert_eq!(page.record_count(), 2);

        // Compact
        page.compact();

        // Should now have 2 slots with renumbered records
        assert_eq!(page.slot_count(), 2);
        assert_eq!(page.record_count(), 2);

        // Records should still be accessible (but with new slot IDs)
        let records: Vec<_> = page.records().collect();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_fill_ratio() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        let ratio_empty = page.fill_ratio();
        assert!(ratio_empty < 0.01);

        // Insert a large record
        let data = vec![0u8; 1000];
        page.insert_record(&data).unwrap();

        let ratio_with_data = page.fill_ratio();
        assert!(ratio_with_data > ratio_empty);
    }

    #[test]
    fn test_page_full() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        // Keep inserting until full
        let large_record = vec![0u8; 1000];
        let mut count = 0;
        while page.insert_record(&large_record).is_some() {
            count += 1;
        }

        // Should have inserted several records
        assert!(count > 0);

        // Trying to insert more should fail
        assert!(page.insert_record(&large_record).is_none());
    }

    #[test]
    fn test_checksum() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);
        page.insert_record(b"Test data").unwrap();
        page.update_checksum();

        assert!(page.verify_checksum());

        // Corrupt the page
        buffer[100] = 0xFF;
        let page = SlottedPage::new(&mut buffer);
        assert!(!page.verify_checksum());
    }

    #[test]
    fn test_slotted_page_ref() {
        let mut buffer = create_test_page();
        {
            let mut page = SlottedPage::new(&mut buffer);
            page.initialize(PageId::new(1), PageType::Leaf);
            page.insert_record(b"A").unwrap();
            page.insert_record(b"B").unwrap();
        }

        let page_ref = SlottedPageRef::new(&buffer);
        assert_eq!(page_ref.slot_count(), 2);
        assert_eq!(page_ref.get_record(0).unwrap(), b"A");
        assert_eq!(page_ref.get_record(1).unwrap(), b"B");
    }

    #[test]
    fn test_empty_record() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        let slot = page.insert_record(b"").unwrap();
        let record = page.get_record(slot).unwrap();
        assert!(record.is_empty());
    }

    #[test]
    fn test_max_record_size() {
        let mut buffer = create_test_page();
        let mut page = SlottedPage::new(&mut buffer);

        page.initialize(PageId::new(1), PageType::Leaf);

        // Try to insert a record that's too large
        let huge_record = vec![0u8; TEST_PAGE_SIZE + 1];
        assert!(page.insert_record(&huge_record).is_none());
    }
}
