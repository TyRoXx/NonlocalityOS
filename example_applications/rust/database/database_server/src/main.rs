#[deny(warnings)]
use database_trait::ExampleDatabase;
use database_trait::ExampleDatabaseRPCServer;
use database_trait::User;
use essrpc::transports::BincodeTransport;
use essrpc::RPCError;
use essrpc::RPCServer;
use nonlocality_env::accept;
use sqlite::State;

struct ExampleDatabaseImpl {
    connection: sqlite::Connection,
}

impl ExampleDatabase for ExampleDatabaseImpl {
    fn create_user(&self, name: String, role: String) -> Result<bool, RPCError> {
        println!("create_user called with {} {}.", &name, &role);
        insert_user_row(&self.connection, &name, &role);
        Ok(true)
    }

    fn list_users(&self) -> Result<Vec<User>, RPCError> {
        Ok(list_user_rows(&self.connection))
    }
}

fn create_database_connection(path: &std::path::Path) -> sqlite::Connection {
    let connection = sqlite::open(path).expect("Tried to create database connection");
    let query = "CREATE TABLE IF NOT EXISTS users (name TEXT, role TEXT)";
    connection.execute(query).expect("Tried to create table");
    connection
}

fn list_user_rows(connection: &sqlite::Connection) -> Vec<User> {
    let query = "SELECT name, role FROM users";
    let mut statement = connection.prepare(query).unwrap();
    let mut users: Vec<User> = Vec::new();
    while let Ok(State::Row) = statement.next() {
        users.push(User {
            name: statement.read::<String, _>("name").unwrap(),
            role: statement.read::<String, _>("role").unwrap(),
        });
    }
    users
}

fn insert_user_row(connection: &sqlite::Connection, name: &str, role: &str) {
    let query = "INSERT INTO users (name, role) VALUES (?, ?)";
    let mut statement = connection
        .prepare(query)
        .expect("Tried to prepare statement");
    statement
        .bind((1, name))
        .expect("Tried to bind parameter to statement");
    statement
        .bind((2, role))
        .expect("Tried to bind parameter to statement");
    loop {
        match statement.next().expect("Tried to execute statement") {
            State::Row => {
                panic!("INSERT should not return a row")
            }
            State::Done => break,
        }
    }
}

#[test]
fn test_insert_user_row() {
    let connection = create_database_connection(std::path::Path::new(":memory:"));
    insert_user_row(&connection, "Alice", "admin");
    insert_user_row(&connection, "Bob", "guest");
    let users = list_user_rows(&connection);
    assert_eq!(
        &[
            User {
                name: "Alice".to_string(),
                role: "admin".to_string()
            },
            User {
                name: "Bob".to_string(),
                role: "guest".to_string()
            },
        ][..],
        &users[..]
    );
}

fn main() {
    println!("Opening the database.");
    let connection = create_database_connection(std::path::Path::new("/database.sqlite"));
    println!("Accepting an API client..");
    let accepted = accept();
    println!(
        "Accepted an API client for interface {}.",
        accepted.interface
    );
    let mut server = ExampleDatabaseRPCServer::new(
        ExampleDatabaseImpl {
            connection: connection,
        },
        BincodeTransport::new(accepted.stream),
    );
    match server.serve() {
        Ok(()) => {
            println!("Serve completed successfully.");
        }
        Err(error) => {
            println!("Serve failed: {}.", error);
        }
    }
}
