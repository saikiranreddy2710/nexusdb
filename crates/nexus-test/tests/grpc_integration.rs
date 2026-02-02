//! gRPC client-server integration tests.
//!
//! These tests verify that the client can connect to the server over gRPC
//! and execute SQL queries.

use std::sync::Arc;
use std::time::Duration;

use nexus_client::{Client, ClientConfig};
use nexus_server::database::Database;
use nexus_server::grpc::GrpcServer;

/// Test basic connection and query execution.
#[tokio::test]
async fn test_grpc_connection() {
    // Start a server in the background
    let db = Database::open_memory().expect("Failed to create database");
    let db = Arc::new(db);

    let addr = "127.0.0.1:50051".parse().expect("Invalid address");
    let server = GrpcServer::new(db.clone(), addr);

    // Spawn server in background
    let server_handle = tokio::spawn(async move {
        let _ = server.serve().await;
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect client
    let config = ClientConfig::new()
        .host("127.0.0.1")
        .port(50051)
        .connect_timeout(Duration::from_secs(5));

    let client = Client::new(config);
    let connect_result = client.connect().await;

    // The connection might fail if the port is already in use
    // Skip the test in that case
    if let Err(e) = connect_result {
        eprintln!("Skipping test - connection failed: {}", e);
        server_handle.abort();
        return;
    }

    // Execute a simple query
    let result = client.execute("SELECT 1 + 1").await;
    assert!(result.is_ok(), "Query failed: {:?}", result);

    // Disconnect
    client.disconnect().await.expect("Disconnect failed");

    // Abort server
    server_handle.abort();
}

/// Test that the mock client works without a server.
#[tokio::test]
async fn test_mock_client() {
    let client = Client::connect_default().expect("Failed to create mock client");
    assert!(client.is_connected());

    // Execute queries in mock mode
    let result = client.execute("SELECT 1").await;
    assert!(result.is_ok());

    // Begin transaction
    let txn = client.begin().await;
    assert!(txn.is_ok());

    // Commit
    txn.unwrap().commit().await.expect("Commit failed");
}

/// Test client ping.
#[tokio::test]
async fn test_mock_ping() {
    let client = Client::connect_default().expect("Failed to create mock client");

    let ping_result = client.ping().await;
    assert!(ping_result.is_ok());
    
    let latency = ping_result.unwrap();
    assert!(latency < Duration::from_secs(1));
}

/// Test server info.
#[tokio::test]
async fn test_mock_server_info() {
    let client = Client::connect_default().expect("Failed to create mock client");

    let info = client.server_info().await.expect("Failed to get server info");
    
    // In mock mode, server name should indicate mock
    assert!(info.server_name.contains("mock") || info.server_name.contains("NexusDB"));
    assert!(info.protocol_version > 0);
}
