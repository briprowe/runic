use std::collections::VecDeque;
use std::time::Duration;

use etcd;
use etcd::kv;
use failure;
use futures;
use futures::sink::Sink;
use futures::stream::{self, Stream};
use hyper::client::connect::Connect;
use tokio::prelude::*;

type LogEntry = String;

pub(crate) fn inbox<C>(
    client: etcd::Client<C>,
    key: &str,
) -> impl Stream<Item = Option<LogEntry>, Error = impl failure::Fail> + '_
where
    C: Clone + Connect,
{
    let options = kv::WatchOptions {
        index: None,
        recursive: true,
        timeout: Some(Duration::from_secs(10)),
    };

    stream::unfold(true, move |is_continue| {
        if !is_continue {
            return None;
        }

        Some(
            kv::watch(&client, key, options).map(|response| match response.data.node.value {
                Some(data) => (Some(data), true),
                _ => (None, false),
            }),
        )
    })
}

struct PutLogEntry {
    item: LogEntry,
    tries: usize,
    request: Option<Box<dyn Future<Item = (), Error = ()> + Send>>,
}

impl PutLogEntry {
    fn new(item: LogEntry) -> Self {
        Self {
            item,
            tries: 0,
            request: None,
        }
    }

    fn request<C>(&mut self, client: &etcd::Client<C>, key: &str)
    where
        C: Clone + Connect,
    {
        let request = kv::set(client, key, &self.item, None)
            .and_then(|_| Ok(()))
            .or_else(|_| Err(()));

        self.tries += 1;
        self.request
            .replace(Box::new(tokio::spawn(request).into_future()));
    }
}

pub(crate) struct Outbox<'a, C: Clone + Connect + 'static> {
    client: etcd::Client<C>,
    key: &'a str,
    output_entries: VecDeque<PutLogEntry>,
}

impl<'a, C> Outbox<'a, C>
where
    C: Clone + Connect,
{
    pub(crate) fn new(client: etcd::Client<C>, key: &'a str) -> Self {
        Self {
            client,
            key,
            output_entries: Default::default(),
        }
    }
}

impl<'a, C> Sink for Outbox<'a, C>
where
    C: Clone + Connect,
{
    type SinkItem = LogEntry;
    type SinkError = (); //Vec<etcd::Error>;

    fn start_send(
        &mut self,
        item: Self::SinkItem,
    ) -> futures::StartSend<Self::SinkItem, Self::SinkError> {
        println!("Queuing request for item: {}", item);
        self.output_entries.push_back(PutLogEntry::new(item));
        Ok(futures::AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        while let Some(mut put_request) = self.output_entries.pop_front() {
            println!("Checking progress on: {}", put_request.item);
            if let Some(ref mut request) = put_request.request {
                match request.poll() {
                    Ok(futures::Async::Ready(_response)) => {
                        println!("Got response for {}", put_request.item);
                        continue;
                    }
                    Ok(futures::Async::NotReady) => {
                        println!("{} not ready", put_request.item);
                        self.output_entries.push_front(put_request);
                        return Ok(futures::Async::NotReady);
                    }
                    Err(e) => return Err(e),
                }
            } else {
                put_request.request(&self.client, &self.key);
                println!(
                    "Making request #{} for item {}",
                    put_request.tries, put_request.item
                );
                self.output_entries.push_front(put_request);
            }
        }
        Ok(futures::Async::Ready(()))
    }
}
