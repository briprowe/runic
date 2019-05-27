use std::process;

use etcd::Client;
use futures::stream::Stream;
use tokio::runtime::Runtime;

mod inbox;

fn main() {
    let client = Client::new(&["http://localhost:2379"], None).unwrap();
    let key = "/foo";

    let work = inbox::inbox(client, key).for_each(|item| {
        if let Some(message) = item {
            println!("{}", message);
        } else {
            println!("No message...");
        }

        Ok(())
    });


    if let Ok(mut runtime) = Runtime::new() {
        if let Err(error) =  runtime.block_on(work) {
            eprintln!("Error: {}", error);
            process::exit(1);
        }
    } else {
        eprintln!("Could not create tokio runtime.");
        process::exit(1);
    }
}
