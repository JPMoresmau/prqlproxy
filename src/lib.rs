use anyhow::{anyhow, Result};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use clap::Parser;
use lazy_static::lazy_static;
use log::{debug, error, info, trace};
use prql_compiler::{ErrorMessage, ErrorMessages, Options, Target};
use std::io::{Cursor, Seek};
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;

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

/// Ready for query message.
const READY: [u8; 6] = [b'Z', 0, 0, 0, 5, b'I'];

lazy_static! {
    static ref OPTIONS: Options = Options::default()
        .with_signature_comment(false)
        .with_target(Target::Sql(Some(prql_compiler::sql::Dialect::PostgreSql,)));
}

/// Start the proxy on the given address with the given upstream, sending a signal when accepting connections.
pub async fn start(address: &str, upstream: &str, sx: Sender<()>) -> io::Result<()> {
    let listener = TcpListener::bind(&address).await?;
    info!("Listening on {}", address);
    let _ = sx.send(());
    loop {
        let (client, _) = listener.accept().await?;
        let server = TcpStream::connect(upstream).await?;
        info!("Connected to {}", upstream);
        let (mut client_r, client_w) = client.into_split();
        let (mut server_r, mut server_w) = server.into_split();

        let client_w1 = Arc::new(Mutex::new(client_w));
        let client_w2 = Arc::clone(&client_w1);

        tokio::spawn(async move {
            let mut buf = vec![0; 4096];
            client_r.readable().await.unwrap();
            let mut state = State::Start;
            loop {
                match client_r.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(count) => {
                        trace!("read {count} bytes from client");
                        match intercept_client(&mut client_r, &state, &mut buf, count).await {
                            Ok((_, _, Some(msgs))) => {
                                for msg in msgs.inner {
                                    match prql_error_message(&msg).await {
                                        Ok(dt) => {
                                            match client_w2.lock().await.write_all(&dt).await {
                                                Ok(_) => {}
                                                Err(err) => error!("Error queuing to client {err}"),
                                            }
                                        }
                                        Err(err) => error!("Error generating error message: {err}"),
                                    }
                                }
                                match client_w2.lock().await.write_all(&READY).await {
                                    Ok(_) => {}
                                    Err(err) => error!("Error queuing to client {err}"),
                                };
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
                    Ok(count) => match client_w1.lock().await.write_all(&buf[0..count]).await {
                        Ok(_) => {}
                        Err(err) => error!("Error queuing to client {err}"),
                    },
                    Err(err) => {
                        error!("Error reading from server {err}");
                        return;
                    }
                }
            }
        });
    }
}

/// Intercept the client message, if it looks like a PRQL query rewrite it as SQL.
async fn intercept_client(
    client_r: &mut OwnedReadHalf,
    state: &State,
    buf: &mut Vec<u8>,
    mut count: usize,
) -> Result<(State, usize, Option<ErrorMessages>)> {
    let mut c = Cursor::new(&buf[..count]);
    match state {
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
            // First message can be SSL negotiation, we don't support that for now.
            if protocol == 80877103 {
                Ok((State::Start, count, None))
            } else {
                Ok((State::Content, count, None))
            }
        }
        // Normal message.
        _ => {
            let op = ReadBytesExt::read_u8(&mut c)? as char;
            trace!("client operation {op}");
            let mut msgs = None;
            // Query.
            if op == 'Q' {
                let _sz = ReadBytesExt::read_i32::<BigEndian>(&mut c)?;
                //let target = sz as usize + 1;

                let (query, _r) = read_string(&mut c)?; //read_query_string(client_r, count, target, sz as usize - 4, &mut c).await?;
                debug!("simple query: {query}");
                // PRQL query, as recognized by the prefix.
                if let Some(prql) = query.strip_prefix("prql:") {
                    let prql = prql.trim().trim_end_matches(';');
                    match prql_compiler::compile(prql, &OPTIONS) {
                        Ok(sql) => {
                            debug!("prql transformed to {sql}");
                            let bs = sql.as_bytes();
                            let mut c = Cursor::new(buf);
                            // Same Query operation.
                            WriteBytesExt::write_u8(&mut c, op as u8)?;
                            // Initial message size (4) + query size + final semi colon + final zero.
                            WriteBytesExt::write_u32::<BigEndian>(&mut c, (bs.len() + 6) as u32)?;
                            count = c.write(bs).await?;
                            WriteBytesExt::write_u8(&mut c, b';')?;
                            WriteBytesExt::write_u8(&mut c, 0)?;
                            // Full message size + operation flag.
                            count += 7;
                        }
                        Err(err) => {
                            error!("prql error: {err}");
                            msgs = Some(err);
                        }
                    };
                }
            // Parse prepared statement.
            } else if op == 'P' {
                let sz = ReadBytesExt::read_u32::<BigEndian>(&mut c)? as usize;
                let (statement, _sz2) = read_string(&mut c)?;
                let (query, _sz3) = read_string(&mut c)?;
                debug!("simple query: {query}");
                // PRQL query, as recognized by the prefix.
                if let Some(prql) = query.strip_prefix("prql:") {
                    let prql = prql.trim().trim_end_matches(';');

                    let fld_nb = ReadBytesExt::read_u16::<BigEndian>(&mut c)? as usize;
                    let mut flds = Vec::with_capacity(fld_nb);
                    for _ in 0..fld_nb {
                        flds.push(ReadBytesExt::read_u32::<BigEndian>(&mut c)?);
                    }
                    let extra = Vec::from(&buf[sz+1..count]);
                    
                    match prql_compiler::compile(prql, &OPTIONS) {
                        Ok(sql) => {
                            debug!("prql transformed to {sql}");
                            let bs = sql.as_bytes();
                            let sbs = statement.as_bytes();
                            // Operation + Initial message size (4) + sz2 (includes zero) + query + final semi colon + final zero + field count (2) + field types.
                            count = bs.len() + sbs.len() + 10 + (fld_nb * 4) + extra.len();
                            let mut c = Cursor::new(buf);
                            // Same Parse operation.
                            WriteBytesExt::write_u8(&mut c, b'P')?;
                            WriteBytesExt::write_u32::<BigEndian>(&mut c, (count - 1 - extra.len()) as u32)?;
                            c.write(sbs).await?;
                            WriteBytesExt::write_u8(&mut c, 0)?;
                            c.write(bs).await?;
                            WriteBytesExt::write_u8(&mut c, b';')?;
                            WriteBytesExt::write_u8(&mut c, 0)?;
                            WriteBytesExt::write_u16::<BigEndian>(&mut c, fld_nb as u16)?;
                            for f in flds {
                                WriteBytesExt::write_u32::<BigEndian>(&mut c, f)?;
                            }
                            c.write(&extra).await?;
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

async fn read_query_string(
    client_r: &mut OwnedReadHalf,
    mut count: usize,
    target: usize,
    sz: usize,
    c: &mut Cursor<&[u8]>,
) -> Result<(String, usize)> {
    if count < target {
        // Query longer than buffer, keep reading to get the proper full size.
        let mut full_buf = Vec::with_capacity(sz);
        let mut query_c: Cursor<&mut Vec<u8>> = Cursor::new(&mut full_buf);
        query_c.write_all(&c.get_ref()[0..count]).await?;
        while count < target {
            let mut query_buf = vec![0; 4096];
            match client_r.read(&mut query_buf).await {
                Ok(0) => {
                    return Err(anyhow!("Client closed while reading query"));
                }
                Ok(n) => {
                    count += n;
                    query_c.write_all(&query_buf[0..n]).await?;
                }
                Err(err) => {
                    return Err(anyhow!("Error reading rest of query from client {err}"));
                }
            }
        }
        query_c.set_position(c.position());
        read_string(&mut query_c)
    } else {
        read_string(c)
    }
}

/// Generate a Postgres error message for a PRQL error.
async fn prql_error_message(msg: &ErrorMessage) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(100);
    let mut c = Cursor::new(&mut buf);
    WriteBytesExt::write_u8(&mut c, b'E')?;
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
    Ok(buf)
}

/// Write a single error field with the given code and value.
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

// Communication state between client and server.
#[derive(Debug)]
enum State {
    Start,
    Content,
}
