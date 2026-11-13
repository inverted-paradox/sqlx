use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::ext::ustr::UStr;
use crate::types::Oid;
use crate::{HashMap, PgConnectOptions, PgTypeInfo};

/// Custom types looked up in `pg_type`, shared by every connection given the same cache.
///
/// Each connection resolves the types it has not seen (enums, domains, composites, ranges and
/// their arrays) with a query on first use. Connections made with
/// [`PgConnectOptions::shared_type_cache`][crate::PgConnectOptions::shared_type_cache] start
/// with what the others resolved and add what they resolve, so a type is looked up once per
/// cache instead of once per connection. A pool clones its options for every connection, so
/// one cache in the options serves the whole pool; cloning a `PgTypeCache` shares it.
///
/// Types are kept apart per database (host, port, socket, database, user and `options`):
/// a type OID only means something in the database that assigned it. A type dropped and
/// created again gets a new OID; [`clear`][Self::clear] the cache after changing types under a
/// running process.
#[derive(Clone, Default)]
pub struct PgTypeCache {
    databases: Arc<Mutex<HashMap<String, SharedTypes>>>,
}

pub(crate) type SharedTypes = Arc<Mutex<TypeMaps>>;

#[derive(Default)]
pub(crate) struct TypeMaps {
    pub(crate) type_oid: HashMap<UStr, Oid>,
    pub(crate) type_info: HashMap<Oid, PgTypeInfo>,
    pub(crate) elem_type_to_array: HashMap<Oid, Oid>,
}

impl PgTypeCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Forgets every type. Open connections keep what they already have.
    pub fn clear(&self) {
        for types in lock(&self.databases).values() {
            *lock(types) = TypeMaps::default();
        }
    }

    /// The types of the database `options` connect to.
    pub(crate) fn database(&self, options: &PgConnectOptions) -> SharedTypes {
        let key = format!(
            "{}\0{}\0{:?}\0{:?}\0{}\0{:?}",
            options.host,
            options.port,
            options.socket,
            options.database,
            options.username,
            options.options,
        );

        lock(&self.databases).entry(key).or_default().clone()
    }
}

impl Debug for PgTypeCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgTypeCache")
            .field("databases", &lock(&self.databases).len())
            .finish()
    }
}

/// Every write is a single insert, so the maps stay consistent across a panic.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl TypeMaps {
    /// Copies into `self` what `other` has and `self` does not; returns whether anything was
    /// new.
    pub(crate) fn merge_from(&mut self, other: &TypeMaps) -> bool {
        let before = self.len();

        for (name, oid) in &other.type_oid {
            self.type_oid.entry_ref(name).or_insert(*oid);
        }

        for (oid, info) in &other.type_info {
            self.type_info.entry(*oid).or_insert_with(|| info.clone());
        }

        for (elem, array) in &other.elem_type_to_array {
            self.elem_type_to_array.entry(*elem).or_insert(*array);
        }

        self.len() != before
    }

    fn len(&self) -> usize {
        self.type_oid.len() + self.type_info.len() + self.elem_type_to_array.len()
    }
}

#[test]
fn databases_are_kept_apart() {
    let cache = PgTypeCache::new();
    let options = PgConnectOptions::new_without_pgpass().database("one");

    let one = cache.database(&options);
    one.lock()
        .unwrap()
        .type_oid
        .insert(UStr::Static("mood"), Oid(16385));

    assert!(Arc::ptr_eq(&one, &cache.database(&options.clone())));

    let two = cache.database(&options.clone().database("two"));
    assert!(!Arc::ptr_eq(&one, &two));
    assert!(two.lock().unwrap().type_oid.is_empty());

    let other_user = cache.database(&options.clone().username("someone"));
    assert!(!Arc::ptr_eq(&one, &other_user));

    cache.clear();
    assert!(one.lock().unwrap().type_oid.is_empty());
}
