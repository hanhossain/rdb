#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    println!("Hello, world!");
    Ok(())
}
