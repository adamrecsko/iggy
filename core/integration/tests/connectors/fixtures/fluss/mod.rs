mod cluster;
mod sink;

use integration::harness::TestBinaryError;
pub use sink::FlussSinkFixture;

fn fixture_error(message: String) -> TestBinaryError {
    TestBinaryError::FixtureSetup {
        fixture_type: "FlussCluster".to_string(),
        message,
    }
}
