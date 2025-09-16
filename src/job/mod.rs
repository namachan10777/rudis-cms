use std::{collections::HashSet, fmt::Debug, path::PathBuf};

use derive_debug::Dbg;
use futures::{future::try_join_all, join};
use indexmap::IndexMap;
use serde::Deserialize;
use serde_with::{json::JsonString, serde_as};

mod sql;
pub mod storage;

use crate::{
    process_data::{self, StorageContent, StoragePointer},
    schema,
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
    pub content: StorageContent,
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
    ) -> impl Future<Output = Result<IndexMap<blake3::Hash, process_data::StoragePointer>, Self::Error>>;
    fn sync(
        &self,
        ctx: &Self::Context,
        tables: &process_data::table::Tables,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

pub struct JobExecutor<D, K, R, A> {
    pub d1: D,
    pub kv: K,
    pub r2: R,
    pub asset: A,
}

#[derive(Debug, thiserror::Error)]
pub enum JobError<DE, KE, OE, AE> {
    #[error("database: {0}")]
    Database(DE),
    #[error("kv: {0}")]
    Kv(KE),
    #[error("objstore: {0}")]
    ObjectStorage(OE),
    #[error("asset: {0}")]
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

fn filter_uploads<T>(
    uploads: impl Iterator<Item = process_data::table::Upload>,
    present_objects: &IndexMap<blake3::Hash, T>,
    force: bool,
) -> impl Iterator<Item = process_data::table::Upload> {
    uploads.filter_map(move |upload| {
        if force || !present_objects.contains_key(&upload.hash) {
            Some(upload)
        } else {
            None
        }
    })
}

fn disappeared_objects<'a, T>(
    present_objects: IndexMap<blake3::Hash, process_data::StoragePointer>,
    appeared_objects: &'a IndexMap<blake3::Hash, T>,
    mask: &'a HashSet<StoragePointer>,
) -> impl 'a + Iterator<Item = StoragePointer> {
    present_objects
        .into_iter()
        .filter(|(hash, pointer)| !appeared_objects.contains_key(hash) && !mask.contains(pointer))
        .map(|(_, pointer)| pointer)
}

fn multiplex_upload(
    uploads: impl Iterator<Item = process_data::table::Upload>,
) -> (Vec<R2Upload>, Vec<KvUpload>, Vec<AssetUpload>) {
    let mut r2 = Vec::new();
    let mut kv = Vec::new();
    let mut asset = Vec::new();
    uploads.for_each(|upload| match upload.pointer {
        StoragePointer::Asset { path } => asset.push(AssetUpload {
            path,
            body: upload.data.into(),
        }),
        StoragePointer::Inline { .. } => {}
        StoragePointer::Kv { namespace, key } => kv.push(KvUpload {
            namespace,
            key,
            content: upload.data,
        }),
        StoragePointer::R2 { bucket, key } => r2.push(R2Upload {
            key,
            bucket,
            body: upload.data.into(),
            content_type: upload.content_type,
        }),
    });
    (r2, kv, asset)
}

fn multiplex_delete(
    disappeards: impl Iterator<Item = StoragePointer>,
) -> (Vec<R2Delete>, Vec<KvDelete>, Vec<AssetDelete>) {
    let mut r2 = Vec::new();
    let mut kv = Vec::new();
    let mut asset = Vec::new();
    disappeards.for_each(|pointer| match pointer {
        StoragePointer::R2 { bucket, key } => r2.push(R2Delete { bucket, key }),
        StoragePointer::Asset { path } => asset.push(AssetDelete { path }),
        StoragePointer::Kv { namespace, key } => kv.push(KvDelete { namespace, key }),
        StoragePointer::Inline { .. } => {}
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
        IndexMap<blake3::Hash, process_data::StoragePointer>,
        JobError<D::Error, K::Error, O::Error, A::Error>,
    > {
        #[serde_as]
        #[derive(Deserialize)]
        struct Row {
            #[serde(deserialize_with = "deserialize_hash")]
            hash: blake3::Hash,
            #[serde_as(as = "JsonString")]
            storage: process_data::StoragePointer,
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
            let pair = storage::kv::Pair::builder().key(upload.key);
            let pair = match upload.content {
                StorageContent::Bytes(bin) => pair.binary_value(&bin),
                StorageContent::Text(text) => pair.string_value(text),
            };
            namespaces
                .entry(upload.namespace.clone())
                .or_default()
                .push(pair.build().unwrap());
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
        tables: &process_data::table::Tables,
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

    async fn create_tables_if_not_exist(&self, sqls: &sql::SqlStatements) -> Result<(), D::Error> {
        self.d1.query::<(), &str>(&sqls.ddl, &[]).await?;
        Ok(())
    }

    pub async fn batch(
        &self,
        schema: &schema::CollectionSchema,
        tables: &process_data::table::Tables,
        uploads: process_data::table::Uploads,
        force: bool,
    ) -> Result<(), JobError<D::Error, K::Error, O::Error, A::Error>> {
        let ctx = sql::SqlStatements::new(schema);
        self.create_tables_if_not_exist(&ctx)
            .await
            .map_err(JobError::Database)?;
        let present_objects = self.fetch_objects_metadata(&ctx).await?;
        let delete_mask = uploads
            .iter()
            .map(|upload| &upload.pointer)
            .cloned()
            .collect::<HashSet<_>>();
        let uploads = filter_uploads(uploads.into_iter(), &present_objects, force);

        let (r2, kv, asset) = multiplex_upload(uploads);

        let (upload_r2, upload_kv, upload_asset) = join!(
            self.upload_objstore(r2.into_iter()),
            self.upload_kv(kv.into_iter()),
            self.upload_asset(asset.into_iter()),
        );
        upload_r2.map_err(JobError::ObjectStorage)?;
        upload_kv.map_err(JobError::Kv)?;
        upload_asset.map_err(JobError::Asset)?;

        self.full_sync_db(&ctx, tables)
            .await
            .map_err(JobError::Database)?;

        let appeared_objects = self.fetch_objects_metadata(&ctx).await?;
        let deletions = disappeared_objects(present_objects, &appeared_objects, &delete_mask);
        let (r2, kv, asset) = multiplex_delete(deletions);
        let (delete_objstore, delete_kv, delete_asset) = join!(
            self.delete_objstore(r2.into_iter()),
            self.delete_kv(kv.into_iter()),
            self.delete_asset(asset.into_iter()),
        );
        delete_objstore.map_err(JobError::ObjectStorage)?;
        delete_kv.map_err(JobError::Kv)?;
        delete_asset.map_err(JobError::Asset)?;

        Ok(())
    }
}
