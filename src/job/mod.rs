use std::{collections::HashSet, fmt::Debug, path::PathBuf};

use derive_debug::Dbg;
use indexmap::IndexMap;
use tracing::info;

pub mod cloudflrae;

use crate::{
    field::{self, StoragePointer, upload::Uploads},
    schema,
    table::{self, Tables},
};

#[derive(Hash, PartialEq, Eq)]
pub struct R2Delete {
    pub bucket: String,
    pub key: String,
}

#[derive(Hash, PartialEq, Eq)]
pub struct KvDelete {
    pub namespace: String,
    pub key: String,
}

#[derive(Hash, PartialEq, Eq)]
pub struct AssetDelete {
    pub path: PathBuf,
}

#[derive(Dbg)]
pub struct KvUpload {
    pub namespace: String,
    pub key: String,
    #[dbg(skip)]
    pub content: String,
}

#[derive(Dbg)]
pub struct R2Upload {
    pub bucket: String,
    pub key: String,
    #[dbg(skip)]
    pub body: Box<[u8]>,
    pub content_type: String,
}

#[derive(Dbg)]
pub struct AssetUpload {
    pub path: PathBuf,
    #[dbg(skip)]
    pub body: Box<[u8]>,
}

pub struct Deletions {
    pub r2: Vec<R2Delete>,
    pub kv: Vec<KvDelete>,
    pub asset: Vec<AssetDelete>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error<ES, ED> {
    #[error("Storage error: {0}")]
    Storage(ES),
    #[error("Database error: {0}")]
    Database(ED),
}

pub trait StorageBackend {
    type Error: std::error::Error + Debug + Sync + Send + 'static;
    fn upload(
        &self,
        r2: impl Iterator<Item = R2Upload>,
        kv: impl Iterator<Item = KvUpload>,
        asset: impl Iterator<Item = AssetUpload>,
    ) -> impl Future<Output = Result<(), Self::Error>>;
    fn delete(
        &self,
        r2: impl Iterator<Item = R2Delete>,
        kv: impl Iterator<Item = KvDelete>,
        asset: impl Iterator<Item = AssetDelete>,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

pub trait Database {
    type Context;
    type Error: std::error::Error + Debug + Sync + Send + 'static;
    fn create_context(
        &self,
        schema: &schema::CollectionSchema,
    ) -> impl Future<Output = Result<Self::Context, Self::Error>>;
    fn fetch_objects_metadata(
        &self,
        ctx: &Self::Context,
    ) -> impl Future<Output = Result<IndexMap<blake3::Hash, StoragePointer>, Self::Error>>;
    fn sync(
        &self,
        ctx: &Self::Context,
        tables: &Tables,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

pub struct SyncSet {
    pub(crate) tables: table::Tables,
    pub(crate) uploads: field::upload::Uploads,
}

pub async fn batch<S, D, ES, ED>(
    backend: &S,
    db: &D,
    schema: &schema::CollectionSchema,
    set: SyncSet,
    force: bool,
) -> Result<(), Error<ES, ED>>
where
    S: StorageBackend<Error = ES>,
    D: Database<Error = ED>,
{
    let SyncSet { tables, uploads } = set;
    let Uploads { r2, kv, asset } = uploads;
    let ctx = db.create_context(schema).await.map_err(Error::Database)?;
    let present_objects = db
        .fetch_objects_metadata(&ctx)
        .await
        .map_err(Error::Database)?;
    info!(len = present_objects.len(), "present object hash fetched");
    let kv_delete_mask = kv
        .iter()
        .map(|(_, obj)| KvDelete {
            namespace: obj.namespace.clone(),
            key: obj.key.clone(),
        })
        .collect::<HashSet<_>>();

    let r2_delete_mask = r2
        .iter()
        .map(|(_, obj)| R2Delete {
            bucket: obj.bucket.clone(),
            key: obj.key.clone(),
        })
        .collect::<HashSet<_>>();

    let asset_delete_mask = asset
        .iter()
        .map(|(_, obj)| AssetDelete {
            path: obj.path.clone(),
        })
        .collect::<HashSet<_>>();

    let r2 = r2.into_iter().filter_map(|(hash, obj)| {
        if force || !present_objects.contains_key(&hash) {
            Some(obj)
        } else {
            None
        }
    });
    let kv = kv.into_iter().filter_map(|(hash, obj)| {
        if force || !present_objects.contains_key(&hash) {
            Some(obj)
        } else {
            None
        }
    });
    let asset = asset.into_iter().filter_map(|(hash, obj)| {
        if force || !present_objects.contains_key(&hash) {
            Some(obj)
        } else {
            None
        }
    });
    backend
        .upload(r2, kv, asset)
        .await
        .map_err(Error::Storage)?;
    info!("upload task finished");
    db.sync(&ctx, &tables).await.map_err(Error::Database)?;
    info!("upsert finished");
    let appeared_objects = db
        .fetch_objects_metadata(&ctx)
        .await
        .map_err(Error::Database)?;
    let mut asset_delete_list = Vec::new();
    let mut kv_delete_list = Vec::new();
    let mut r2_delete_list = Vec::new();
    present_objects.into_iter().for_each(|(hash, pointer)| {
        if appeared_objects.contains_key(&hash) {
            return;
        }
        match pointer {
            StoragePointer::Asset { path } => {
                let delete = AssetDelete { path };
                if asset_delete_mask.contains(&delete) {
                    return;
                }
                asset_delete_list.push(delete);
            }
            StoragePointer::Kv { namespace, key } => {
                let delete = KvDelete { namespace, key };
                if kv_delete_mask.contains(&delete) {
                    return;
                }
                kv_delete_list.push(delete);
            }
            StoragePointer::R2 { bucket, key } => {
                let delete = R2Delete { bucket, key };
                if r2_delete_mask.contains(&delete) {
                    return;
                }
                r2_delete_list.push(delete);
            }
        }
    });
    backend
        .delete(
            r2_delete_list.into_iter(),
            kv_delete_list.into_iter(),
            asset_delete_list.into_iter(),
        )
        .await
        .map_err(Error::Storage)?;
    info!("cleanup finished");

    Ok(())
}
