fn descriptor(project: &str) -> StreamDesc {
    crate::registry::PersistedDescriptor {
        name: "canonical-cache".into(),
        account_id: None,
        project_id: ProjectId::new(project).unwrap(),
        stream_epoch: "11".repeat(16),
        seal_gen_counter: 0,
        key_fingerprint: String::new(),
        created_ms: 0,
        expires_at_ms: None,
        deleted: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: vec![],
        init: None,
        sealing: None,
        seal_op: None,
        content_type: "application/octet-stream".into(),
        ttl_secs: None,
        segments: None,
        sealed: false,
        watch_definitions: vec![],
        watch_sig_key: None,
        parent_ref_pending: false,
        layout_version: crate::registry::LAYOUT_VERSION,
    }
    .try_into()
    .unwrap()
}
