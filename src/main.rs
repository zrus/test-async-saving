use anyhow::Result as AnyResult;
use bastion::prelude::*;
use tokio::io::AsyncWriteExt;
use tracing::{warn, Level};

const PATH: &'static str = "test";

#[derive(Debug)]
struct Frame {
    order: u64,
    frame: Vec<u8>,
}

#[cfg(multi_thread)]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> AnyResult<()> {
    core().await
}

#[cfg(not(multi_thread))]
#[tokio::main]
async fn main() -> AnyResult<()> {
    core().await
}

async fn core() -> AnyResult<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    Bastion::init();
    Bastion::start();

    Bastion::supervisor(|sp| {
        sp.children(|c| {
            c.with_distributor(Distributor::named("file_writter"))
                .with_exec(|ctx: BastionContext| async move {
                    warn!("file writter spawned!");
                    loop {
                        MessageHandler::new(ctx.recv().await?).on_tell(|msg: Frame, _| {
                            spawn!(async move {
                                warn!("received frame: {:?}", msg.order);
                                let mut file =
                                    tokio::fs::File::create(format!("{}/{}.txt", PATH, msg.order))
                                        .await
                                        .expect("cannot create file");
                                file.write_all(&msg.frame)
                                    .await
                                    .expect("cannot write to file");
                                file.flush().await.expect("cannot flush data");
                                drop(file);
                                warn!("write completed!");
                            })
                        });
                    }
                })
        })
    })
    .expect("cannot create file writter");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    Bastion::supervisor(|sp| {
        sp.children(|c| {
            c.with_exec(|_ctx: BastionContext| async move {
                warn!("sender spawned!");
                let mut order = 1u64;
                loop {
                    let contents = vec![0u8; 25 * 1024];
                    let frame = Frame {
                        order,
                        frame: contents,
                    };
                    let file_writter = Distributor::named("file_writter");
                    file_writter
                        .tell_one(frame)
                        .expect("cannot send to file writter");
                    order += 1;
                    // warn!("send completed: {}", order);
                }
            })
        })
    })
    .expect("cannot create sender");

    Bastion::block_until_stopped();
    Ok(())
}
