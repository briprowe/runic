use etcd::Client;
use futures::sink::Sink;
use futures::stream::Stream;
use std::process;
use std::time::{Duration, Instant};
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::timer::Delay;

mod comm;

fn main() {
    let inbox_client = Client::new(&["http://localhost:2379"], None).unwrap();
    let key = "/foo";

    let work = comm::inbox(inbox_client, key).for_each(|item| {
        if let Some(message) = item {
            println!("Received: {}", message);
        } else {
            println!("No message...");
        }

        Ok(())
    });

    if let Ok(mut runtime) = Runtime::new() {
        let outbox_client = Client::new(&["http://localhost:2379"], None).unwrap();
        let outbox = comm::Outbox::new(outbox_client, key);
        let when = Instant::now() + Duration::from_secs(5);

        let sending_work = Delay::new(when)
            .map_err(|e| {
                eprintln!("Error waiting on delay: {:?}", e);
                process::exit(1);
            })
            .and_then(|_| {
                outbox.send_all(
                    stream::iter_ok::<_, ()>(vec!["foo", "bar", "baz"]).map(|i| i.to_string()),
                )
            })
            // .flatten()
            .and_then(|_| Ok(()))
            .or_else(|e| {
                eprintln!("Error while sending: {:?}", e);
                Err(())
            });

        runtime.spawn(sending_work);

        if let Err(error) = runtime.block_on(work) {
            eprintln!("Error: {}", error);
            process::exit(1);
        }
    } else {
        eprintln!("Could not create tokio runtime.");
        process::exit(1);
    }
}
