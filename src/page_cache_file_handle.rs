use std::{collections::BTreeMap, ops::Range, sync::RwLock};

use futures::future::try_join_all;
use tantivy::{
    directory::{FileHandle, OwnedBytes},
    HasLen,
};
use tokio::runtime::Handle;

const PAGE_SIZE: usize = 4096;

pub struct PageCacheFileHandle {
    handle: Handle,
    inner: Box<dyn FileHandle>,
    cache: RwLock<BTreeMap<u32, OwnedBytes>>,
}

impl PageCacheFileHandle {
    pub fn new(handle: Handle, inner: Box<dyn FileHandle>) -> Self {
        Self {
            handle,
            inner,
            cache: RwLock::new(BTreeMap::new()),
        }
    }

    fn fetch_pages_into_cache(&self, pages: &[u32]) -> std::io::Result<()> {
        if pages.is_empty() {
            return Ok(());
        }

        let mut merged_ranges = Vec::new();
        let mut start_page = pages[0];
        let mut end_page = start_page + 1;

        for &page in pages.iter().skip(1) {
            if page == end_page {
                end_page = page + 1;
            } else {
                merged_ranges.push((start_page, end_page));
                start_page = page;
                end_page = page + 1;
            }
        }
        merged_ranges.push((start_page, end_page));

        let read_futures = merged_ranges
            .iter()
            .map(|(start_page, end_page)| {
                let start = *start_page as usize * PAGE_SIZE;
                let end = *end_page as usize * PAGE_SIZE;
                self.inner.read_bytes_async(start..end)
            })
            .collect::<Vec<_>>();

        let results = self.handle.block_on(try_join_all(read_futures))?;

        for ((start_page, end_page), bytes) in merged_ranges.into_iter().zip(results) {
            let mut cache = self.cache.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Cache lock is poisoned.")
            })?;

            let page_range_len = (end_page - start_page) as usize;
            for (i, page) in (start_page..end_page).enumerate() {
                let page_end = if i == page_range_len - 1 && bytes.len() % PAGE_SIZE != 0 {
                    i * PAGE_SIZE + bytes.len() % PAGE_SIZE
                } else {
                    (i + 1) * PAGE_SIZE
                };
                let page_bytes = OwnedBytes::new(bytes[i * PAGE_SIZE..page_end].to_vec());
                cache.insert(page, page_bytes);
            }
        }

        Ok(())
    }
}

impl std::fmt::Debug for PageCacheFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PageCacheFileHandle({})", &self.inner.len())
    }
}

fn range_to_pages(range: &Range<usize>) -> Vec<u32> {
    let mut pages = Vec::new();
    let start_page = range.start / PAGE_SIZE;
    let end_page = (range.end + PAGE_SIZE - 1) / PAGE_SIZE;

    for page in start_page..end_page {
        pages.push(page as u32);
    }

    pages
}

impl FileHandle for PageCacheFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        let pages = range_to_pages(&range);
        let num_pages = pages.len();
        assert_ne!(num_pages, 0);

        let missing_pages = {
            let cache = self.cache.read().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Cache lock is poisoned.")
            })?;

            pages
                .iter()
                .filter(|page| !cache.contains_key(page))
                .copied()
                .collect::<Vec<_>>()
        };

        self.fetch_pages_into_cache(&missing_pages)?;

        let mut bytes = Vec::with_capacity(range.len());
        {
            let cache = self.cache.read().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Cache lock is poisoned.")
            })?;

            for (i, page) in pages.into_iter().enumerate() {
                let page_bytes = cache
                    .get(&page)
                    .unwrap_or_else(|| panic!("just added page {page}, yet not found in cache?"));

                let start = if i == 0 { range.start % PAGE_SIZE } else { 0 };

                let end = if i == num_pages - 1 && range.end % PAGE_SIZE != 0 {
                    range.end % PAGE_SIZE
                } else {
                    PAGE_SIZE
                };

                bytes.extend_from_slice(&page_bytes[start..end]);
            }
        }

        Ok(OwnedBytes::new(bytes))
    }
}

impl HasLen for PageCacheFileHandle {
    fn len(&self) -> usize {
        self.inner.len()
    }
}
