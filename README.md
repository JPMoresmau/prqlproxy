# PRQL Postgres Reverse Proxy

[PRQL](https://github.com/PRQL/prql) is a modern SQL replacement.

This proxy sits in front of a Postgres instance and intercepts queries: queries that are prefixed by `prql:` are
treated as PRQL instead of SQL: they are converted to SQL on the fly by the proxy, and the generated SQL is sent
to the server for processing. If the PRQL conversion fails, errors are returned as PSQL errors.

 ## Usage

`prqlproxy --server <SERVER> --address <ADDRESS>`

```
Options:
  -s, --server <SERVER>    Address:port of the PostgreSQL database server to proxy
  -a, --address <ADDRESS>  Address:port of the listening proxy
  -h, --help               Print help
  -V, --version            Print version
```

## Example

Start the proxy `prqlproxy -s localhost:5432 -a localhost:6142`

Connect from psql: 

```
psql -h localhost -p 6142 -d pagila -U postgres
Password for user postgres: 
psql (14.7 (Homebrew))
Type "help" for help.

pagila=# prql: from customer
pagila-# select [first_name, last_name]
pagila-# take 1..20;
 first_name | last_name 
------------+-----------
 MARY       | SMITH
 PATRICIA   | JOHNSON
 LINDA      | WILLIAMS
 BARBARA    | JONES
 ELIZABETH  | BROWN
 JENNIFER   | DAVIS
 MARIA      | MILLER
 SUSAN      | WILSON
 MARGARET   | MOORE
 DOROTHY    | TAYLOR
 LISA       | ANDERSON
 NANCY      | THOMAS
 KAREN      | JACKSON
 BETTY      | WHITE
 HELEN      | HARRIS
 SANDRA     | MARTIN
 DONNA      | THOMPSON
 CAROL      | GARCIA
 RUTH       | MARTINEZ
 SHARON     | ROBINSON
(20 rows)

pagila=# prql: I don't know what I'm doing;
ERROR:  Unknown name `I`
```

## Development

It's a very basic reverse proxy built in Rust with Tokio and the PRQL Rust library.

SSL is not supported.
