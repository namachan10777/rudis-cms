use std::{collections::HashSet, fmt::Debug, path::PathBuf};

use derive_debug::Dbg;
use futures::{future::try_join_all, join};
use indexmap::IndexMap;
use serde::Deserialize;
use serde_with::{json::JsonString, serde_as};

mod sql;
pub mod storage;

use crate::{
    field::{self, StoragePointer},
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

pub struct JobExecutor<D, K, R, A> {
    d1: D,
    kv: K,
    r2: R,
    asset: A,
}

pub enum JobError<DE, KE, OE, AE> {
    Database(DE),
    Kv(KE),
    ObjectStorage(OE),
    Asset(AE),
}

fn deserialize_hash<'de, D>(deserializer: D) -> Result<blake3::Hash, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use std::str::FromStr as _;
    let s = String::deserialize(deserializer)?;
    blake3::Hash::from_str(&s).map_err(serde::de::Error::custom)
}

fn kv_delete_mask<'a>(uploads: impl Iterator<Item = &'a KvUpload>) -> HashSet<KvDelete> {
    uploads
        .map(|obj| KvDelete {
            namespace: obj.namespace.clone(),
            key: obj.key.clone(),
        })
        .collect::<HashSet<_>>()
}

fn objstore_delete_mask<'a>(uploads: impl Iterator<Item = &'a R2Upload>) -> HashSet<R2Delete> {
    uploads
        .map(|obj| R2Delete {
            bucket: obj.bucket.clone(),
            key: obj.key.clone(),
        })
        .collect::<HashSet<_>>()
}

fn asset_delete_mask<'a>(uploads: impl Iterator<Item = &'a AssetUpload>) -> HashSet<AssetDelete> {
    uploads
        .map(|obj| AssetDelete {
            path: obj.path.clone(),
        })
        .collect::<HashSet<_>>()
}

fn filter_uploads<T, U>(
    uploads: impl Iterator<Item = (blake3::Hash, T)>,
    present_objects: &IndexMap<blake3::Hash, U>,
    force: bool,
) -> impl Iterator<Item = T> {
    uploads.filter_map(move |(hash, obj)| {
        if force || !present_objects.contains_key(&hash) {
            Some(obj)
        } else {
            None
        }
    })
}

fn disappeared_objects<T>(
    present_objects: IndexMap<blake3::Hash, StoragePointer>,
    appeared_objects: &IndexMap<blake3::Hash, T>,
    r2_mask: &HashSet<R2Delete>,
    kv_mask: &HashSet<KvDelete>,
    asset_mask: &HashSet<AssetDelete>,
) -> (Vec<R2Delete>, Vec<KvDelete>, Vec<AssetDelete>) {
    let mut r2 = Vec::new();
    let mut kv = Vec::new();
    let mut asset = Vec::new();
    present_objects.into_iter().for_each(|(hash, pointer)| {
        if !appeared_objects.contains_key(&hash) {
            return;
        }
        match pointer {
            StoragePointer::R2 { bucket, key } => {
                let object = R2Delete { bucket, key };
                if !r2_mask.contains(&object) {
                    r2.push(object);
                }
            }
            StoragePointer::Kv { namespace, key } => {
                let object = KvDelete { namespace, key };
                if !kv_mask.contains(&object) {
                    kv.push(object);
                }
            }
            StoragePointer::Asset { path } => {
                let object = AssetDelete { path };
                if !asset_mask.contains(&object) {
                    asset.push(object);
                }
            }
        }
    });
    (r2, kv, asset)
}

impl<
    D: storage::sqlite::Client,
    K: storage::kv::Client,
    O: storage::r2::Client,
    A: storage::asset::Client,
> JobExecutor<D, K, O, A>
{
    async fn fetch_objects_metadata(
        &self,
        sqls: &sql::SqlStatements,
    ) -> Result<
        IndexMap<blake3::Hash, StoragePointer>,
        JobError<D::Error, K::Error, O::Error, A::Error>,
    > {
        #[serde_as]
        #[derive(Deserialize)]
        struct Row {
            #[serde(deserialize_with = "deserialize_hash")]
            hash: blake3::Hash,
            #[serde_as(as = "JsonString")]
            storage: StoragePointer,
        }
        let objects = self
            .d1
            .query::<Row, &str>(&sqls.fetch_objects, &[])
            .await
            .map_err(JobError::Database)?
            .into_iter()
            .map(|row| (row.hash, row.storage))
            .collect::<IndexMap<_, _>>();
        Ok(objects)
    }

    async fn upload_objstore(
        &self,
        uploads: impl Iterator<Item = R2Upload>,
    ) -> Result<(), O::Error> {
        let tasks = uploads.map(|upload| {
            self.r2.put(
                upload.bucket,
                upload.key,
                upload.content_type,
                upload.body.into_vec().into(),
            )
        });
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn upload_kv(&self, uploads: impl Iterator<Item = KvUpload>) -> Result<(), K::Error> {
        let mut namespaces = IndexMap::<_, Vec<_>>::new();
        for upload in uploads {
            namespaces
                .entry(upload.namespace.clone())
                .or_default()
                .push(
                    storage::kv::Pair::builder()
                        .key(upload.key)
                        .string_value(upload.content)
                        .build()
                        .unwrap(),
                );
        }
        for (namespace, pairs) in namespaces {
            self.kv.write_multiple(&namespace, &pairs).await?;
        }
        Ok(())
    }

    async fn upload_asset(
        &self,
        uploads: impl Iterator<Item = AssetUpload>,
    ) -> Result<(), A::Error> {
        let tasks =
            uploads.map(|asset| async move { self.asset.put(&asset.path, &asset.body).await });
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn delete_objstore(
        &self,
        deletes: impl Iterator<Item = R2Delete>,
    ) -> Result<(), O::Error> {
        let tasks = deletes
            .into_iter()
            .map(|delete| self.r2.delete(delete.bucket, delete.key));
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn delete_kv(&self, deletes: impl Iterator<Item = KvDelete>) -> Result<(), K::Error> {
        let mut namespaces = IndexMap::<_, Vec<_>>::new();
        for delete in deletes {
            namespaces
                .entry(delete.namespace)
                .or_default()
                .push(delete.key);
        }
        let tasks = namespaces.into_iter().map(|(namespace, keys)| async move {
            self.kv.delete_multiple(&namespace, &keys).await
        });
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn delete_asset(
        &self,
        assets: impl Iterator<Item = AssetDelete>,
    ) -> Result<(), A::Error> {
        let tasks = assets.map(|asset| async move { self.asset.delete(&asset.path).await });
        try_join_all(tasks).await?;
        Ok(())
    }

    async fn full_sync_db(
        &self,
        sqls: &sql::SqlStatements,
        tables: &table::Tables,
    ) -> Result<(), D::Error> {
        let param = serde_json::to_string(tables).expect("tables must be encodable");
        for statement in &sqls.upsert {
            self.d1
                .query::<(), _>(statement, &[&param.as_str()])
                .await?;
        }
        self.d1
            .query::<(), _>(&sqls.cleanup, &[&param.as_str()])
            .await?;
        Ok(())
    }

    pub async fn batch(
        &self,
        schema: &schema::CollectionSchema,
        tables: &table::Tables,
        uploads: field::upload::Uploads,
        force: bool,
    ) -> Result<(), JobError<D::Error, K::Error, O::Error, A::Error>> {
        let ctx = sql::SqlStatements::new(schema);
        let present_objects = self.fetch_objects_metadata(&ctx).await?;
        let kv_delete_mask = kv_delete_mask(uploads.kv.values());
        let r2_delete_mask = objstore_delete_mask(uploads.r2.values());
        let asset_delete_mask = asset_delete_mask(uploads.asset.values());
        let r2 = filter_uploads(uploads.r2.into_iter(), &present_objects, force);
        let kv = filter_uploads(uploads.kv.into_iter(), &present_objects, force);
        let asset = filter_uploads(uploads.asset.into_iter(), &present_objects, force);

        let (upload_r2, upload_kv, upload_asset) = join!(
            self.upload_objstore(r2),
            self.upload_kv(kv),
            self.upload_asset(asset),
        );
        upload_r2.map_err(JobError::ObjectStorage)?;
        upload_kv.map_err(JobError::Kv)?;
        upload_asset.map_err(JobError::Asset)?;

        self.full_sync_db(&ctx, tables)
            .await
            .map_err(JobError::Database)?;

        let appeared_objects = self.fetch_objects_metadata(&ctx).await?;
        let (r2_deletions, kv_deletions, asset_deletions) = disappeared_objects(
            present_objects,
            &appeared_objects,
            &r2_delete_mask,
            &kv_delete_mask,
            &asset_delete_mask,
        );
        let (delete_objstore, delete_kv, delete_asset) = join!(
            self.delete_objstore(r2_deletions.into_iter()),
            self.delete_kv(kv_deletions.into_iter()),
            self.delete_asset(asset_deletions.into_iter()),
        );
        delete_objstore.map_err(JobError::ObjectStorage)?;
        delete_kv.map_err(JobError::Kv)?;
        delete_asset.map_err(JobError::Asset)?;

        Ok(())
    }
}
