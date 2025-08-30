use url::Url;

use crate::{
    config::{self, ImageFormat},
    field::object_loader::{self, SvgNode},
    record::CompoundId,
};

struct ImageVariant {
    width: u32,
    height: u32,
    format: ImageFormat,
    url: Option<url::Url>,
}

pub struct ImageLoctaor<'i> {
    image: &'i object_loader::Image,
    id: CompoundId,
    width: Option<u32>,
    url: Option<url::Url>,
    image_format: ImageFormat,
    variants: Vec<ImageVariant>,
}

trait RasterImageLocator {
    fn locate_default(&self, width: u32, format: ImageFormat) -> Url;
    fn locate_variant(&self, width: u32, format: ImageFormat) -> Url;
}

trait VectorImageLocator {
    fn locate(&self) -> Url;
}

trait FileLocator {
    fn locate(&self) -> Url;
}

pub struct RasterImage {
    pub data: image::DynamicImage,
    pub hash: blake3::Hash,
    pub origin: object_loader::Origin,
    pub derived_id: String,
}

pub struct VectorImage {
    pub data: SvgNode,
    pub dimention: (u32, u32),
    pub hash: blake3::Hash,
    pub origin: Option<url::Url>,
    pub derived_id: String,
}

pub trait RecordBackend {
    type Error;
    fn raster_image_locator(&self, id: &CompoundId, image: &RasterImage)
    -> impl RasterImageLocator;
    fn vector_image_locator(&self, id: &CompoundId, image: &VectorImage)
    -> impl VectorImageLocator;
    fn file_locator(&self, id: &CompoundId, file: &object_loader::Object) -> impl FileLocator;
}
