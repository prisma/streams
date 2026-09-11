use super::*;
type ProducerPlane = ([u8; 16], String);
type ProducerRecord = (u64, u64, u64, [u8; 16]);
#[derive(Default)]
pub(super) struct ProducerOverlay {
    pub rows: HashMap<ProducerPlane, ProducerRecord>,
    pub seqs: HashMap<[u8; 16], String>,
}
#[derive(Default)]
pub(super) struct QueueOverlay {
    pub state: Option<crate::queue::QueueState>,
    // mt-lint: allow(name-keyed-map): consumer names are scoped by the transaction's tenant-qualified stream hash
    pub configs: HashMap<String, crate::queue::ConsumerRecord>,
}
#[derive(Default)]
pub(super) struct BillingOverlay {
    pub meta: Option<crate::billing::SegmentBillingMetaV1>,
    pub dirty: bool,
    pub month_finals: Vec<crate::billing::SegmentSnapshot>,
}
#[derive(Default)]
pub(super) struct FrameEffects {
    pub payload_bytes: u64,
    pub added_bytes: u64,
    pub retired_bytes: u64,
    pub ring: Vec<(u64, Bytes)>,
}
pub(super) struct StreamOverlay {
    pub handle: Arc<StreamHandle>,
    pub fields: TailFields,
    pub base: TailFields,
    pub producer: ProducerOverlay,
    pub queue: QueueOverlay,
    pub billing: BillingOverlay,
    pub frames: FrameEffects,
}
impl StreamOverlay {
    pub(super) fn new(
        handle: Arc<StreamHandle>,
        billing: Option<crate::billing::SegmentBillingMetaV1>,
    ) -> Self {
        let fields = handle.state.lock().unwrap().applied.clone();
        Self {
            handle,
            base: fields.clone(),
            fields,
            producer: ProducerOverlay::default(),
            queue: QueueOverlay::default(),
            billing: BillingOverlay {
                meta: billing,
                ..Default::default()
            },
            frames: FrameEffects::default(),
        }
    }
}
