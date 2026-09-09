//! Optional request-correlated diagnostic. Nested durations are NOT additive.
use std::{sync::{Arc,Mutex},time::Instant};
tokio::task_local! {static STAGES: Arc<Mutex<Vec<(&'static str,u64)>>>;}
pub(crate) async fn scope<F:std::future::Future>(id:Option<String>,future:F)->F::Output {
    let Some(id)=id else{return future.await};
    let rows=Arc::new(Mutex::new(Vec::new()));let started=Instant::now();
    let result=STAGES.scope(rows.clone(),future).await;
    println!("SERVER_STAGES {}",serde_json::json!({"request_id":id,"application_response_us":started.elapsed().as_micros() as u64,"nested_stages":*rows.lock().unwrap()}));result
}
pub(crate) struct Guard(Option<(Arc<Mutex<Vec<(&'static str,u64)>>>,&'static str,Instant)>);
pub(crate) fn enter(name:&'static str)->Guard {Guard(STAGES.try_with(|r|(r.clone(),name,Instant::now())).ok())}
impl Drop for Guard {fn drop(&mut self){if let Some((r,n,t))=&self.0 {r.lock().unwrap().push((n,t.elapsed().as_micros() as u64));}}}
