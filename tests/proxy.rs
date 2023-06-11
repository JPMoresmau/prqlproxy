use anyhow::Result;
use lazy_static::lazy_static;
use tokio::runtime::{Builder, Runtime};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use prqlproxy::start;

/// Create a new client connected to the proxy.
async fn test_client() -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost user=postgres password=password dbname=pagila port=6142",
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}

lazy_static! {
    /// Start a tokio runtime and start the text proxy.
    static ref RUNTIME: Result<Runtime> = {
        env_logger::init();
        let runtime = Builder::new_current_thread().enable_io().build()?;
        runtime.block_on(async {
            let (sx, rx) = tokio::sync::oneshot::channel();
            tokio::spawn(async {
                if let Err(err) = start("localhost:6142", "localhost:5432", sx).await {
                    panic!("cannot start proxy: {err}");
                }
            });
            rx.await
        })?;
        Ok(runtime)
    };
}

#[test]
fn test_simple_sql_query() -> Result<()> {
    RUNTIME.as_ref().unwrap().block_on(async {
        let rows = test_client()
            .await?
            .simple_query("select count(*) from customer")
            .await?;
        if let Some(SimpleQueryMessage::Row(r)) = rows.get(0) {
            assert_eq!("599", r.get(0).unwrap());
        } else {
            panic!("no row");
        }
        Ok(())
    })
}

#[test]
fn test_extended_sql_query() -> Result<()> {
    RUNTIME.as_ref().unwrap().block_on(async {
        let row = test_client()
            .await?
            .query_one("select count(*) from customer", &[])
            .await?;
        assert_eq!(599, row.get::<usize, i64>(0));
        Ok(())
    })
}

#[test]
fn test_simple_prql_query() -> Result<()> {
    RUNTIME.as_ref().unwrap().block_on(async {
        let rows = test_client()
            .await?
            .simple_query("prql: from customer\naggregate [ct = count]")
            .await?;
        if let Some(SimpleQueryMessage::Row(r)) = rows.get(0) {
            assert_eq!("599", r.get(0).unwrap());
        } else {
            panic!("no row");
        }

        Ok(())
    })
}

#[test]
fn test_simple_prql_query_syntax_error() -> Result<()> {
    RUNTIME.as_ref().unwrap().block_on(async {
        let r = test_client()
            .await?
            .simple_query("prql: wrong")
            .await;
        assert!(r.is_err());
        if let Err(err) = r {
            assert!(err.to_string().contains("ERROR: Unknown name wrong"));
        }
        Ok(())
    })
}

#[test]
fn test_extended_prql_query() -> Result<()> {
    RUNTIME.as_ref().unwrap().block_on(async {
        let row = test_client()
            .await?
            .query_one("prql: from customer\naggregate [ct = count]", &[])
            .await?;
        assert_eq!(599, row.get::<usize, i64>(0));

        Ok(())
    })
}
