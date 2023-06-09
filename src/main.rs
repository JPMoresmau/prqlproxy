use clap::Parser;

use prqlproxy::start;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Address:port of the PostgreSQL database server to proxy.
    #[arg(short, long)]
    server: String,

    /// Address:port of the listening proxy.
    #[arg(short, long)]
    address: String,
}

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let (sx, _) = tokio::sync::oneshot::channel();
    start(&args.address, &args.server, sx).await
}
