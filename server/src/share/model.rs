use fory_derive::ForyObject;
use std::sync::Arc;

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
