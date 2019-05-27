use std::time::Duration;

use futures::stream::{self,Stream};
use etcd;
use etcd::kv;
use failure;
use tokio::prelude::*;
use hyper::client::connect::Connect;

type LogEntry = String;

pub(crate) fn inbox<C>(client: etcd::Client<C>, key: &str) -> impl Stream<Item = Option<LogEntry>, Error = impl failure::Fail> + '_
where C: Clone + Connect
{
    stream::unfold(true, move |is_continue| {
        if ! is_continue {
            return None;
        }

        let options = kv::WatchOptions {
            index: None,
            recursive: true,
            timeout: Some(Duration::from_secs(5))
        };

        Some(kv::watch(&client, key, options)
             .map(|response| {
                 match response.data.node.value {
                     Some(data) => (Some(data), true),
                     _ => (None, false)
                 }
             })
             .map_err(|err| {
                 err
             }))
    })
}
