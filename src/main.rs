use anyhow::Result;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use clap::Parser;
use lazy_static::lazy_static;
use log::{debug, error, info, trace};
use prql_compiler::{ErrorMessage, ErrorMessages, Options, Target};
use std::io::{Cursor, Seek};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

// TODOs
// query bigger than 1024?
// return PRQL error to client without hitting the server.

#[derive(Parser, Debug)]
#[command(author="JP Moresmau", version, about="A TCP proxy that can translate PRQL to Postgres SQL", long_about = None)]
struct Args {
    /// Address:port of the PostgreSQL database server to proxy.
    #[arg(short, long)]
    server: String,

    /// Address:port of the listening proxy.
    #[arg(short, long)]
    address: String,
}

lazy_static! {
    static ref OPTIONS: Options = Options::default()
        .with_signature_comment(false)
        .with_target(Target::Sql(Some(prql_compiler::sql::Dialect::PostgreSql,)));
}

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::init();

    let args = Args::parse();

    let listener = TcpListener::bind(&args.address).await?;
    info!("Listening on {}", args.address);
    loop {
        let (client, _) = listener.accept().await?;
        let server = TcpStream::connect(&args.server).await?;
        info!("Connected to {}", args.server);

        let (mut client_r, mut client_w) = client.into_split();
        let (mut server_r, mut server_w) = server.into_split();

        let (tx, mut rx) = mpsc::channel(100);
        let tx2 = tx.clone();

        tokio::spawn(async move {
            let mut buf = vec![0; 4096];
            client_r.readable().await.unwrap();
            let mut state = State::SSL;
            loop {
                match client_r.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(count) => {
                        trace!("read {count} bytes from client");
                        match intercept_client(&state, &mut buf, count).await {
                            Ok((_, _, Some(msgs))) => {
                                for msg in msgs.inner {
                                    match prql_error_message(&msg).await {
                                        Ok(dt) => {
                                            match tx.send(dt).await {
                                                Ok(_) => {}
                                                Err(err) => error!("Error queuing to client {err}"),
                                            }
                                        }
                                        Err(err) => error!("Error generating error message: {err}"),
                                    }
                                }
                                match ready_for_query().await {
                                    Ok(dt) => match tx.send(dt).await
                                    {
                                        Ok(_) => {}
                                        Err(err) => error!("Error queuing to client {err}"),
                                    },
                                    Err(err) => error!("Error generating ready message: {err}"),
                                }
                            }
                            Ok((new_state, new_count, _)) => {
                                state = new_state;
                                match server_w.write_all(&buf[..new_count]).await {
                                    Ok(_) => {}
                                    Err(err) => error!("Error writing to server {err}"),
                                }
                            }
                            Err(err) => error!("Error processing client request: {err}"),
                        }
                       
                    }
                    Err(err) => {
                        error!("Error reading from client {err}");
                        return;
                    }
                }
            }
        });

        tokio::spawn(async move {
            let mut buf = vec![0; 4096];
            loop {
                match server_r.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(count) => {
                        /*let mut c = Cursor::new(&buf[..count]);
                        let op = ReadBytesExt::read_u8(&mut c).unwrap() as char;
                        trace!("server operation {op}");
                        if op == 'T'{
                            let mut _sz = ReadBytesExt::read_i32::<BigEndian>(&mut c).unwrap();
                            let fs = ReadBytesExt::read_i16::<BigEndian>(&mut c).unwrap();
                            for _ in 0..fs {
                                let (field_name,_) = read_string(&mut c);
                                let _tbl = ReadBytesExt::read_i32::<BigEndian>(&mut c).unwrap();
                                let _col = ReadBytesExt::read_i16::<BigEndian>(&mut c).unwrap();
                                let dt = ReadBytesExt::read_i32::<BigEndian>(&mut c).unwrap();
                                let _dtsz = ReadBytesExt::read_i16::<BigEndian>(&mut c).unwrap();
                                let _mod = ReadBytesExt::read_i32::<BigEndian>(&mut c).unwrap();
                                let _fmt = ReadBytesExt::read_i16::<BigEndian>(&mut c).unwrap();
                                debug!("field: {field_name} ({dt})");
                            }
                        }*/
                        match tx2.send(buf[0..count].into()).await {
                            Ok(_) => {}
                            Err(err) => error!("Error queuing to client {err}"),
                        }
                    }
                    Err(err) => {
                        error!("Error reading from server {err}");
                        return;
                    }
                }
            }
        });

        tokio::spawn(async move {
            while let Some(i) = rx.recv().await {
                match client_w.write_all(&i).await {
                    Ok(_) => {}
                    Err(err) => error!("Error writing to client {err}"),
                }
            }
        });
    }
}

async fn intercept_client(
    state: &State,
    mut buf: &mut Vec<u8>,
    mut count: usize,
) -> Result<(State, usize, Option<ErrorMessages>)> {
    let mut c = Cursor::new(&buf[..count]);
    match state {
        // First message is SSL negotiation, we don't support that for now.
        State::SSL => {
            let sz = ReadBytesExt::read_u32::<BigEndian>(&mut c)?;
            let protocol = ReadBytesExt::read_i32::<BigEndian>(&mut c)?;
            trace!("ssl {sz} {protocol}");
            Ok((State::Start, count, None))
        }
        // Client start message.
        State::Start => {
            let mut sz = ReadBytesExt::read_i32::<BigEndian>(&mut c)?;
            let protocol = ReadBytesExt::read_i32::<BigEndian>(&mut c)?;
            trace!("start {sz} {protocol}");
            sz -= 8;
            while sz > 0 {
                let (s1, r) = read_string(&mut c)?;
                sz -= r as i32;
                if sz > 0 {
                    let (s2, r) = read_string(&mut c)?;
                    sz -= r as i32;
                    trace!("{s1}: {s2}");
                }
            }
            Ok((State::Content, count, None))
        }
        // Normal message.
        _ => {
            let op = ReadBytesExt::read_u8(&mut c)? as char;
            trace!("client operation {op}");
            let mut msgs = None;
            // Query.
            if op == 'Q' {
                let _sz = ReadBytesExt::read_i32::<BigEndian>(&mut c)?;
                let (s1, _r) = read_string(&mut c)?;
                debug!("query: {s1}");
                // PRQL query, as recognized by the prefix.
                if let Some(prql) = s1.strip_prefix("prql:") {
                    let prql = prql.trim().trim_end_matches(";");
                    match prql_compiler::compile(prql, &OPTIONS) {
                        Ok(sql) => {
                            debug!("prql transformed to {sql}");
                            let bs = sql.as_bytes();
                            buf.clear();
                            // Same Query operation.
                            WriteBytesExt::write_u8(&mut buf, op as u8)?;
                            // Query size + initial message size (4) + final semi colon + final zero.
                            WriteBytesExt::write_u32::<BigEndian>(&mut buf, (bs.len() + 6) as u32)?;
                            count = buf.write(bs).await?;
                            WriteBytesExt::write_u8(&mut buf, ';' as u8)?;
                            WriteBytesExt::write_u8(&mut buf, 0)?;
                            // Full message size + operation flag.
                            count += 7;
                        }
                        Err(err) => {
                            error!("prql error: {err}");
                            msgs = Some(err);
                        }
                    };
                }
            }
            Ok((State::Content, count, msgs))
        }
    }
}

async fn ready_for_query() -> Result<Vec<u8>>{
    let mut buf = Vec::with_capacity(6);
    WriteBytesExt::write_u8(&mut buf, 'Z' as u8)?;
    WriteBytesExt::write_u32::<BigEndian>(&mut buf, 5)?;
    WriteBytesExt::write_u8(&mut buf, 'I' as u8)?;
    Ok(buf)
}

async fn prql_error_message(msg: &ErrorMessage) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(100);
    let mut c = Cursor::new(&mut buf);
    WriteBytesExt::write_u8(&mut c, 'E' as u8)?;
    let mut sz = 5_u32;
    WriteBytesExt::write_u32::<BigEndian>(&mut c, sz)?;
    sz += write_field(&mut c, 'S', "ERROR").await?;
    sz += write_field(&mut c, 'V', "ERROR").await?;
    // Syntax error.
    sz += write_field(&mut c, 'C', "42601").await?;
    sz += write_field(&mut c, 'M', &msg.reason).await?;
    if let Some(code) = &msg.code {
        sz += write_field(&mut c, 'D', code).await?;
    }

    if let Some(hint) = &msg.hint {
        sz += write_field(&mut c, 'H', hint).await?;
    }
    if let Some(span) = &msg.span {
        sz += write_field(&mut c, 'P', &span.start.to_string()).await?;
    }
    if let Some(location) = &msg.location {
        sz += write_field(&mut c, 'L', &location.start.0.to_string()).await?;
    }
    WriteBytesExt::write_u8(&mut c, 0)?;

    // Set proper size.
    c.seek(io::SeekFrom::Start(1))?;
    WriteBytesExt::write_u32::<BigEndian>(&mut c, sz)?;
    eprintln!("{} {buf:?}", buf.len());
    Ok(buf)
}

async fn write_field(c: &mut Cursor<&mut Vec<u8>>, code: char, value: &str) -> Result<u32> {
    WriteBytesExt::write_u8(c, code as u8)?;
    let mut sz = 2;
    sz += c.write(value.as_bytes()).await? as u32;
    WriteBytesExt::write_u8(c, 0)?;
    Ok(sz)
}

/// Read a zero delimited string.
fn read_string<R>(c: &mut R) -> Result<(String, usize)>
where
    R: ReadBytesExt,
{
    let mut v = Vec::new();
    let mut i = c.read_u8()?;
    while i > 0 {
        v.push(i);
        i = c.read_u8()?;
    }
    let sz = v.len() + 1;
    let s = String::from_utf8(v)?;
    Ok((s, sz))
}

enum State {
    SSL,
    Start,
    Content,
}
