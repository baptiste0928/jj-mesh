//! Packfile generation and ingestion for the pack transfer format.
//!
//! Generation runs gix-pack's entries -> bytes pipeline over the object ids
//! the closure walk collected. Deltas already stored in the server's
//! on-disk packs are copied as-is; other objects are recompressed as base
//! objects (gix-pack does not create new deltas). Ingestion streams the
//! pack into `objects/pack`, verifying the trailer checksum and hashing
//! every object while building the index, and persists `.pack` and `.idx`
//! atomically: a partially received pack never becomes visible.
//!
//! A received pack is peer-supplied data: its header is bounded here
//! before gix allocates from it (see [`MAX_PACK_OBJECTS`]).
//!
//! Both directions run blocking gix I/O and bridge to the async frame
//! stream through the caller's channels, via [`ChunkReader`] and the chunk
//! emit callback.

use std::{
    io,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

use color_eyre::eyre::{Result, WrapErr as _, ensure, eyre};
use gix::progress::{Count, Id, MessageLevel, NestedProgress, Progress, Step, StepShared, UNKNOWN};
use tokio::sync::mpsc;

/// Pack bytes are buffered into chunks of this size before each emit, so
/// the wire sees few large frames instead of one per written entry.
const PACK_CHUNK_SIZE: usize = 1 << 20;

/// Cap on the object count a received pack may declare in its header.
///
/// gix sizes its delta tree from this count before reading a single entry,
/// so it bounds an allocation a peer names for free. Generous next to any
/// history a mesh repo replicates; a legitimate pack past it fails with a
/// clear error instead of the fetcher dying.
const MAX_PACK_OBJECTS: u32 = 1 << 22;

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
    let entry_count = u32::try_from(ids.len()).wrap_err("more objects than a pack can hold")?;
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
        return Err(eyre!("cannot write pack: {err}"));
    }
    writer.finish()
}

/// Buffers pack bytes into chunks for the emit callback, surfacing emit
/// errors through `failed` (io::Write flattens them to `io::Error`).
struct ChunkWriter<F: FnMut(Vec<u8>) -> Result<()>> {
    emit: F,
    buffer: Vec<u8>,
    failed: Option<color_eyre::Report>,
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

/// Objects-indexed count shared between [`ingest_pack`]'s gix pipeline and
/// the async receive loop, which samples it for progress display.
#[derive(Clone, Default)]
pub(super) struct SharedObjectCount(StepShared);

impl SharedObjectCount {
    /// The count so far.
    pub(super) fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed) as u64
    }
}

/// Minimal prodash progress handed to the gix ingest pipeline: the child
/// counting indexed objects steps the shared count, every other node and
/// signal is discarded. Progress is display-only, so an id drift in a gix
/// upgrade would freeze the counter, never break the ingest.
struct IndexCountProgress {
    objects: StepShared,
    /// Whether this node is the objects counter.
    active: bool,
}

impl IndexCountProgress {
    fn root(objects: StepShared) -> Self {
        IndexCountProgress {
            objects,
            active: false,
        }
    }
}

impl Count for IndexCountProgress {
    fn set(&self, step: Step) {
        if self.active {
            self.objects.store(step, Ordering::Relaxed);
        }
    }

    fn step(&self) -> Step {
        if self.active {
            self.objects.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    fn inc_by(&self, step: Step) {
        if self.active {
            self.objects.fetch_add(step, Ordering::Relaxed);
        }
    }

    fn counter(&self) -> StepShared {
        if self.active {
            self.objects.clone()
        } else {
            StepShared::default()
        }
    }
}

impl Progress for IndexCountProgress {
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

impl NestedProgress for IndexCountProgress {
    type SubProgress = Self;

    fn add_child(&mut self, name: impl Into<String>) -> Self {
        self.add_child_with_id(name, UNKNOWN)
    }

    fn add_child_with_id(&mut self, _name: impl Into<String>, id: Id) -> Self {
        let objects_id: Id = gix_pack::index::write::ProgressId::IndexObjects.into();
        IndexCountProgress {
            objects: self.objects.clone(),
            active: id == objects_id,
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
/// `indexed` is stepped once per object as the index is built.
pub(super) fn ingest_pack(
    git: &gix::Repository,
    mut read: impl io::Read,
    indexed: &SharedObjectCount,
) -> Result<IngestOutcome> {
    // The header's object count is checked before gix-pack sees it: gix
    // preallocates its delta tree from that count, so an unchecked u32
    // turns a 12-byte header into a multi-gigabyte reservation, and an
    // allocation failure aborts the process rather than failing the fetch.
    let mut header = [0u8; 12];
    read.read_exact(&mut header)
        .wrap_err("cannot read pack header")?;
    let (_version, objects) =
        gix::odb::pack::data::header::decode(&header).map_err(|err| eyre!("bad pack: {err}"))?;
    ensure!(
        objects <= MAX_PACK_OBJECTS,
        "pack declares {objects} objects, over the {MAX_PACK_OBJECTS} cap",
    );
    let read = io::Read::chain(io::Cursor::new(header), read);

    let pack_dir = git.objects.store_ref().path().join("pack");
    let no_interrupt = AtomicBool::new(false);
    let mut progress = IndexCountProgress::root(indexed.0.clone());
    let outcome = gix_pack::Bundle::write_to_directory(
        &mut io::BufReader::new(read),
        Some(&pack_dir),
        &mut progress,
        &no_interrupt,
        // The mesh never generates thin packs, so there are no external
        // bases to look up.
        None::<gix::objs::find::Never>,
        gix_pack::bundle::write::Options {
            thread_limit: None,
            iteration_mode: gix_pack::data::input::Mode::Verify,
            index_version: gix_pack::index::Version::default(),
            object_hash: git.object_hash(),
        },
    )
    .map_err(|err| eyre!("cannot ingest pack: {err}"))?;
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
