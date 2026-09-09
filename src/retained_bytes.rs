//! Immutable, exact-capacity bytes whose reservation follows the last alias.
//! Selection/admission belongs to the batch above this primitive: a slice can
//! intentionally retain the full owner, and therefore retains its full charge.
use bytes::Bytes;

pub(crate) fn with_charge<C: Send + 'static>(bytes: Box<[u8]>, charge: C) -> Bytes {
    struct Owner<C> {
        bytes: Box<[u8]>,
        _charge: C,
    }
    impl<C> AsRef<[u8]> for Owner<C> {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    Bytes::from_owner(Owner {
        bytes,
        _charge: charge,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[test]
    fn reservation_follows_the_final_byte_alias() {
        struct Charge(Arc<AtomicUsize>, usize);
        impl Drop for Charge {
            fn drop(&mut self) {
                self.0.fetch_sub(self.1, Ordering::SeqCst);
            }
        }
        let retained = Arc::new(AtomicUsize::new(8192));
        let bytes = with_charge(
            vec![b'x'; 8192].into_boxed_slice(),
            Charge(retained.clone(), 8192),
        );
        let slice = bytes.slice(0..1);
        drop(bytes);
        assert_eq!(retained.load(Ordering::SeqCst), 8192);
        assert_eq!(slice.as_ref(), b"x");
        drop(slice);
        assert_eq!(retained.load(Ordering::SeqCst), 0);
    }
}
