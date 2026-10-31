//! Packfile generation and ingestion for the pack and store transfer
//! formats.
//!
//! Generation runs gix-pack's entries -> bytes pipeline over the object ids
//! the closure walk collected. Deltas already stored in the server's
//! on-disk packs are copied as-is; other objects are recompressed as base
//! objects (gix-pack does not create new deltas). A whole-store transfer
//! instead copies the server's pack files byte for byte (see [`Store`]).
//! Ingestion streams a pack into `objects/pack`, verifying the trailer
//! checksum and hashing every object while building the index, and
//! persists `.pack` and `.idx` atomically: a partially received pack never
//! becomes visible.
//!
//! Both directions run blocking gix I/O and bridge to the async frame
//! stream through the caller's channels, via [`ChunkReader`] and the chunk
//! emit callback.

use std::{
    fs,
    io::{self, Read as _},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use anyhow::{Context as _, Result, anyhow};
use gix::progress::{Count, Id, MessageLevel, NestedProgress, Progress, Step, StepShared, UNKNOWN};
use tokio::sync::mpsc;

/// Pack bytes are buffered into chunks of this size before each emit, so
/// the wire sees few large frames instead of one per written entry.
const PACK_CHUNK_SIZE: usize = 1 << 20;

/// A repo's whole object store, as a store transfer sends it: its pack
/// files as they are, then its loose objects packed.
pub(super) struct Store {
    pub(super) packs: Vec<StoredPack>,
    pub(super) loose: Vec<gix::ObjectId>,
}

/// One pack file of a [`Store`]. Only opened while sent, so a store of
/// many packs holds one file at a time; a pack a concurrent gc removed
/// meanwhile fails the transfer, which the fetcher retries.
pub(super) struct StoredPack {
    path: PathBuf,
    objects: u32,
}

impl Store {
    /// Lists the repo's packs (those with an index) and loose objects.
    /// Blocking.
    pub(super) fn open(git: &gix::Repository) -> Result<Self> {
        let objects_dir = git.objects.store_ref().path().to_owned();
        // Loose objects first: a concurrent gc only moves them into a new
        // pack, which the pack listing then sees, or which serves them
        // through the object database once listed.
        let loose = gix::odb::loose::Store::at(&objects_dir, git.object_hash())
            .iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| anyhow!("cannot list loose objects: {err}"))?;
        let mut packs = Vec::new();
        for entry in fs::read_dir(objects_dir.join("pack")).context("cannot list packs")? {
            let path = entry?.path();
            if path.extension().is_none_or(|ext| ext != "pack")
                || !path.with_extension("idx").exists()
            {
                continue;
            }
            let mut header = [0u8; 12];
            open_pack(&path)?.read_exact(&mut header)?;
            let (_version, objects) = gix::odb::pack::data::header::decode(&header)
                .map_err(|err| anyhow!("bad pack {}: {err}", path.display()))?;
            packs.push(StoredPack { path, objects });
        }
        Ok(Store { packs, loose })
    }

    /// The number of objects the transfer carries.
    pub(super) fn objects(&self) -> u64 {
        let packed: u64 = self.packs.iter().map(|pack| u64::from(pack.objects)).sum();
        packed + self.loose.len() as u64
    }
}

impl StoredPack {
    /// Hands the pack's bytes to `emit` in chunks. Blocking.
    pub(super) fn write(self, mut emit: impl FnMut(Vec<u8>) -> Result<()>) -> Result<()> {
        let mut file = open_pack(&self.path)?;
        loop {
            let mut chunk = Vec::with_capacity(PACK_CHUNK_SIZE);
            (&mut file)
                .take(PACK_CHUNK_SIZE as u64)
                .read_to_end(&mut chunk)?;
            if chunk.is_empty() {
                return Ok(());
            }
            emit(chunk)?;
        }
    }
}

/// Opens a stored pack file.
fn open_pack(path: &Path) -> Result<fs::File> {
    fs::File::open(path).with_context(|| format!("cannot open {}", path.display()))
}

/// Generates a pack containing exactly `ids` and hands its bytes to `emit`
/// in chunks. Blocking.
pub(super) fn write_pack(
    git: &gix::Repository,
    ids: Vec<gix::ObjectId>,
    emit: impl FnMut(Vec<u8>) -> Result<()>,
) -> Result<()> {
    use gix_pack::data::output;

    let object_hash = git.object_hash();
    let mut handle = git.objects.store().to_cache_arc();
    // Entries copied from existing packs reference them by id until the
    // bytes are written; unloading a pack in between would break them.
    handle.prevent_pack_unload();
    handle.ignore_replacements = true;

    // Counts are built straight from the ids rather than through
    // `count::objects`: the walk already emitted every id exactly once, and
    // counting would fully decode every object (applying whole delta
    // chains) only to discard the bytes. Leaving each location
    // `NotLookedUp` lets the entry stage resolve it in parallel with a
    // cheap index lookup.
    let entry_count = u32::try_from(ids.len()).context("more objects than a pack can hold")?;
    let counts: Vec<output::Count> = ids
        .into_iter()
        .map(|id| output::Count {
            id,
            entry_pack_location: output::count::PackLocation::NotLookedUp,
        })
        .collect();

    let entries = gix::parallel::InOrderIter::from(output::entry::iter_from_counts(
        counts,
        handle,
        Box::new(gix::progress::Discard),
        output::entry::iter_from_counts::Options {
            thread_limit: None,
            mode: output::entry::iter_from_counts::Mode::PackCopyAndBaseObjects,
            allow_thin_pack: false,
            chunk_size: 1000,
            version: gix_pack::data::Version::V2,
            compression: gix::zlib::Compression::DEFAULT,
        },
    ));

    let mut writer = ChunkWriter {
        emit,
        buffer: Vec::with_capacity(PACK_CHUNK_SIZE),
        failed: None,
    };
    let bytes = gix_pack::data::output::bytes::FromEntriesIter::new(
        entries,
        &mut writer,
        entry_count,
        gix_pack::data::Version::V2,
        object_hash,
    );
    let mut write_error = None;
    for written in bytes {
        if let Err(err) = written {
            write_error = Some(err);
            break;
        }
    }
    // An emit failure surfaces as an opaque io error in the pipeline; the
    // original error kept by the writer takes precedence.
    if let Some(err) = writer.failed.take() {
        return Err(err);
    }
    if let Some(err) = write_error {
        return Err(anyhow!("cannot write pack: {err}"));
    }
    writer.finish()
}

/// Buffers pack bytes into chunks for the emit callback, surfacing emit
/// errors through `failed` (io::Write flattens them to `io::Error`).
struct ChunkWriter<F: FnMut(Vec<u8>) -> Result<()>> {
    emit: F,
    buffer: Vec<u8>,
    failed: Option<anyhow::Error>,
}

impl<F: FnMut(Vec<u8>) -> Result<()>> ChunkWriter<F> {
    /// Emits the remaining partial chunk.
    fn finish(mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::take(&mut self.buffer);
        (self.emit)(chunk)
    }
}

impl<F: FnMut(Vec<u8>) -> Result<()>> io::Write for ChunkWriter<F> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(data);
        if self.buffer.len() >= PACK_CHUNK_SIZE {
            let chunk = std::mem::replace(&mut self.buffer, Vec::with_capacity(PACK_CHUNK_SIZE));
            if let Err(err) = (self.emit)(chunk) {
                self.failed = Some(err);
                return Err(io::Error::other("emit failed"));
            }
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Object counts of one [`ingest_pack`], shared with the async receive
/// loop, which samples them for progress display.
#[derive(Clone, Default)]
pub(super) struct IngestCounts {
    indexed: StepShared,
    resolved: StepShared,
}

impl IngestCounts {
    /// Objects indexed so far, as the pack streams in.
    pub(super) fn indexed(&self) -> u64 {
        self.indexed.load(Ordering::Relaxed) as u64
    }

    /// Objects resolved so far, once the pack is in.
    pub(super) fn resolved(&self) -> u64 {
        self.resolved.load(Ordering::Relaxed) as u64
    }
}

/// Minimal prodash progress handed to the gix ingest pipeline: the nodes
/// counting indexed and resolved objects step the matching shared count,
/// every other node and signal is discarded. Progress is display-only, so
/// an id drift in a gix upgrade would freeze a counter, never break the
/// ingest.
struct CountProgress {
    counts: IngestCounts,
    /// The count this node steps, if any.
    counter: Option<StepShared>,
}

impl Count for CountProgress {
    fn set(&self, step: Step) {
        if let Some(counter) = &self.counter {
            counter.store(step, Ordering::Relaxed);
        }
    }

    fn step(&self) -> Step {
        self.counter
            .as_ref()
            .map_or(0, |counter| counter.load(Ordering::Relaxed))
    }

    fn inc_by(&self, step: Step) {
        if let Some(counter) = &self.counter {
            counter.fetch_add(step, Ordering::Relaxed);
        }
    }

    fn counter(&self) -> StepShared {
        self.counter.clone().unwrap_or_default()
    }
}

impl Progress for CountProgress {
    fn init(&mut self, _max: Option<Step>, _unit: Option<gix::progress::Unit>) {}

    fn set_name(&mut self, _name: String) {}

    fn name(&self) -> Option<String> {
        None
    }

    fn id(&self) -> Id {
        UNKNOWN
    }

    fn message(&self, _level: MessageLevel, _message: String) {}
}

impl NestedProgress for CountProgress {
    type SubProgress = Self;

    fn add_child(&mut self, name: impl Into<String>) -> Self {
        self.add_child_with_id(name, UNKNOWN)
    }

    fn add_child_with_id(&mut self, _name: impl Into<String>, id: Id) -> Self {
        use gix_pack::index::write::ProgressId;
        let counter = if id == Id::from(ProgressId::IndexObjects) {
            Some(self.counts.indexed.clone())
        } else if id == Id::from(ProgressId::ResolveObjects) {
            Some(self.counts.resolved.clone())
        } else {
            None
        };
        CountProgress {
            counts: self.counts.clone(),
            counter,
        }
    }
}

/// What a completed pack ingestion produced.
pub(super) struct IngestOutcome {
    /// Objects in the ingested pack.
    pub objects: usize,
    /// Protects the new pack until the apply publishes (see [`PackKeep`]).
    pub keep: PackKeep,
}

/// The `.keep` file gix writes beside a freshly ingested pack, holding git
/// GC off it until refs point into it.
///
/// Removed on drop, so it is released on every path: after a successful
/// apply the keep refs took over, and on a failed or abandoned fetch the
/// pack is unreferenced garbage that GC should be free to reclaim. Leaking
/// it would pin the pack forever, since a retry that finds the identical
/// pack already on disk is not handed the path again.
pub(super) struct PackKeep(Option<PathBuf>);

impl Drop for PackKeep {
    fn drop(&mut self) {
        if let Some(path) = self.0.take()
            && let Err(err) = std::fs::remove_file(&path)
        {
            tracing::warn!("cannot remove pack keep file {}: {err}", path.display());
        }
    }
}

/// Streams a pack from `read` into the repo's `objects/pack`, building its
/// index. Blocking; nothing becomes visible unless the whole pack verifies.
/// `counts` follow the objects as they are indexed, then resolved.
pub(super) fn ingest_pack(
    git: &gix::Repository,
    read: impl io::Read,
    counts: &IngestCounts,
) -> Result<IngestOutcome> {
    let pack_dir = git.objects.store_ref().path().join("pack");
    let no_interrupt = AtomicBool::new(false);
    let mut progress = CountProgress {
        counts: counts.clone(),
        counter: None,
    };
    let outcome = gix_pack::Bundle::write_to_directory(
        &mut io::BufReader::new(read),
        Some(&pack_dir),
        &mut progress,
        &no_interrupt,
        // The mesh never generates thin packs, so there are no external
        // bases to look up.
        None::<gix::objs::find::Never>,
        git.object_hash(),
        gix_pack::bundle::write::Options {
            thread_limit: None,
            iteration_mode: gix_pack::data::input::Mode::Verify,
            index_version: gix_pack::index::Version::default(),
            alloc_limit_bytes: None,
            compression: gix::zlib::Compression::BEST_SPEED,
        },
    )
    .map_err(|err| anyhow!("cannot ingest pack: {err}"))?;
    Ok(IngestOutcome {
        objects: outcome.index.num_objects as usize,
        keep: PackKeep(outcome.keep_path),
    })
}

/// Blocking [`io::Read`] over byte chunks fed by the async receive loop; a
/// closed channel reads as end of stream.
pub(super) struct ChunkReader {
    rx: mpsc::Receiver<Vec<u8>>,
    current: Vec<u8>,
    pos: usize,
}

impl ChunkReader {
    pub(super) fn new(rx: mpsc::Receiver<Vec<u8>>) -> Self {
        ChunkReader {
            rx,
            current: Vec::new(),
            pos: 0,
        }
    }
}

impl io::Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.pos == self.current.len() {
            match self.rx.blocking_recv() {
                Some(chunk) => {
                    self.current = chunk;
                    self.pos = 0;
                }
                None => return Ok(0),
            }
        }
        let n = (self.current.len() - self.pos).min(buf.len());
        buf[..n].copy_from_slice(&self.current[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}
