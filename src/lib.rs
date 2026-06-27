use std::sync::Arc;

pub mod broadcast;
pub mod conn;
pub mod engineio;

pub type AwarenessRef = Arc<yrs::sync::Awareness>;
