use fory_core::Fory;
use fory_derive::ForyObject;
use std::sync::Arc;

pub fn fory_init() -> Result<Arc<Fory>, Box<dyn std::error::Error>> {
    let mut fory = Fory::default();
    fory.register::<PrintTestReq>(1)?;
    fory.register::<PrintTestRes>(2)?;
    fory.register::<SetReq>(3)?;
    fory.register::<SetRes>(4)?;
    fory.register::<GetReq>(5)?;
    fory.register::<GetRes>(6)?;
    fory.register::<DelReq>(7)?;
    fory.register::<DelRes>(8)?;
    fory.register::<ExistsReq>(9)?;
    fory.register::<ExistsRes>(10)?;
    Ok(Arc::new(fory))
}

#[derive(ForyObject, Debug, PartialEq)]
pub struct PrintTestReq {
    pub message: String,
}

#[derive(ForyObject, Debug, PartialEq)]
pub struct PrintTestRes {
    pub message: String,
}

#[derive(ForyObject, Debug, PartialEq)]
pub struct SetReq {
    pub key: String,
    pub value: Vec<u8>,
    pub ex_time: u64,
}
#[derive(ForyObject, Debug, PartialEq)]
pub struct SetRes {}

#[derive(ForyObject, Debug, PartialEq)]
pub struct GetReq {
    pub key: String,
}

#[derive(ForyObject, Debug, PartialEq)]
pub struct GetRes {
    pub value: Option<Arc<Vec<u8>>>,
}

#[derive(ForyObject, Debug, PartialEq)]
pub struct DelReq {
    pub key: String,
}
#[derive(ForyObject, Debug, PartialEq)]
pub struct DelRes {
    pub num: u32,
}

#[derive(ForyObject, Debug, PartialEq)]
pub struct ExistsReq {
    pub key: String,
}
#[derive(ForyObject, Debug, PartialEq)]
pub struct ExistsRes {
    pub num: u32,
}
