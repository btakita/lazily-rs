//! Optional generated Protobuf encoding for canonical graph-boundary facts.
//!
//! Generated messages own representation only. [`GraphBoundaryProjection`]
//! remains the semantic admission layer and produces the same logical state
//! regardless of JSON, msgpack, or Protobuf transport.

use std::collections::BTreeMap;

/// Capability token peers must both advertise before using this encoding.
pub const PROTOBUF_GRAPH_BOUNDARY_FEATURE: &str = "protobuf-graph-boundary-v1";

/// Generated Protobuf wire types.
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/lazily.graph_boundary.v1.rs"));
}

/// A stable cell in the derived logical projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedCell {
    /// Monotonic cell revision.
    pub revision: u64,
    /// Current text.
    pub text: String,
}

/// Semantic result of admitting one boundary envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryDecision {
    /// Applied an ordinary mutation.
    Apply,
    /// Admitted an explicit bootstrap/recovery snapshot.
    Bootstrap,
    /// Folded a remote derived projection.
    Project,
    /// Recorded a host observation without mutating graph input.
    Observe,
    /// Ignored an already-admitted sequence.
    Duplicate,
    /// Rejected an older source generation or causal epoch.
    RejectStale,
    /// Rejected a sequence gap without advancing the watermark.
    RejectGap,
}

/// Semantic admission errors for a decoded graph-boundary envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundaryError {
    /// The envelope carried no body.
    MissingBody,
    /// The graph input carried no concrete input variant.
    MissingInput,
    /// A snapshot purpose was absent or unknown.
    InvalidSnapshotPurpose,
    /// A splice did not match the current cell revision or UTF-8 bounds.
    InvalidSplice,
    /// The envelope carried a boundary family this reducer does not project.
    UnsupportedBody,
}

/// Pure logical reducer behind every negotiated graph-boundary codec.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphBoundaryProjection {
    source_generation: u64,
    causal_epoch: u64,
    last_sequence: u64,
    cells: BTreeMap<String, ProjectedCell>,
}

impl GraphBoundaryProjection {
    /// Read the stable projected cells.
    #[must_use]
    pub fn cells(&self) -> &BTreeMap<String, ProjectedCell> {
        &self.cells
    }

    /// Deterministic logical projection used by conformance and logical hashes.
    #[must_use]
    pub fn logical_projection(&self) -> String {
        self.cells
            .iter()
            .map(|(id, cell)| format!("{id}@{}={}", cell.revision, cell.text))
            .collect::<Vec<_>>()
            .join("|")
    }

    /// Admit one decoded envelope.
    ///
    /// # Errors
    ///
    /// Returns a typed error for malformed or unsupported semantic input.
    pub fn admit(
        &mut self,
        envelope: &wire::ProtocolEnvelope,
    ) -> Result<BoundaryDecision, BoundaryError> {
        let incoming = (envelope.source_generation, envelope.causal_epoch);
        let current = (self.source_generation, self.causal_epoch);
        if incoming < current {
            return Ok(BoundaryDecision::RejectStale);
        }
        if incoming > current {
            self.source_generation = incoming.0;
            self.causal_epoch = incoming.1;
            self.last_sequence = 0;
        }
        if envelope.sequence <= self.last_sequence {
            return Ok(BoundaryDecision::Duplicate);
        }
        if envelope.sequence != self.last_sequence + 1 {
            return Ok(BoundaryDecision::RejectGap);
        }

        let decision = match envelope.body.as_ref().ok_or(BoundaryError::MissingBody)? {
            wire::protocol_envelope::Body::GraphInput(input) => self.apply_input(input)?,
            wire::protocol_envelope::Body::DerivedProjection(projection) => {
                self.cells = projection
                    .cells
                    .iter()
                    .map(|cell| {
                        (
                            cell.cell_id.clone(),
                            ProjectedCell {
                                revision: cell.revision,
                                text: cell.text.clone(),
                            },
                        )
                    })
                    .collect();
                BoundaryDecision::Project
            }
            wire::protocol_envelope::Body::CapabilityHandshake(_)
            | wire::protocol_envelope::Body::EffectIntent(_)
            | wire::protocol_envelope::Body::DeliveryReceipt(_) => {
                return Err(BoundaryError::UnsupportedBody);
            }
        };
        self.last_sequence = envelope.sequence;
        Ok(decision)
    }

    fn apply_input(&mut self, input: &wire::GraphInput) -> Result<BoundaryDecision, BoundaryError> {
        match input.input.as_ref().ok_or(BoundaryError::MissingInput)? {
            wire::graph_input::Input::CellTextSplice(splice) => {
                let cell = self
                    .cells
                    .entry(splice.cell_id.clone())
                    .or_insert(ProjectedCell {
                        revision: 0,
                        text: String::new(),
                    });
                let start = splice.local_offset_utf8 as usize;
                let end = start.saturating_add(splice.delete_length_utf8 as usize);
                if cell.revision != splice.expected_cell_revision
                    || end > cell.text.len()
                    || !cell.text.is_char_boundary(start)
                    || !cell.text.is_char_boundary(end)
                {
                    return Err(BoundaryError::InvalidSplice);
                }
                cell.text.replace_range(start..end, &splice.insert_text);
                cell.revision += 1;
                Ok(BoundaryDecision::Apply)
            }
            wire::graph_input::Input::BootstrapSnapshot(snapshot) => {
                let purpose = wire::SnapshotPurpose::try_from(snapshot.purpose)
                    .map_err(|_| BoundaryError::InvalidSnapshotPurpose)?;
                if purpose == wire::SnapshotPurpose::Unspecified {
                    return Err(BoundaryError::InvalidSnapshotPurpose);
                }
                Ok(BoundaryDecision::Bootstrap)
            }
            wire::graph_input::Input::SurfaceObservation(_) => Ok(BoundaryDecision::Observe),
            wire::graph_input::Input::SourceValueSet(_)
            | wire::graph_input::Input::KeyedMemberUpsert(_)
            | wire::graph_input::Input::KeyedMemberRemove(_) => Err(BoundaryError::UnsupportedBody),
        }
    }

    /// Install canonical cells after a snapshot envelope passes admission.
    pub fn install_snapshot_cells(&mut self, cells: impl IntoIterator<Item = (String, String)>) {
        self.cells = cells
            .into_iter()
            .map(|(id, text)| (id, ProjectedCell { revision: 1, text }))
            .collect();
    }
}
