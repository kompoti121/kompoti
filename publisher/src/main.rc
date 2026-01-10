use anyhow::{Context, Result};
use iroh::{Endpoint, SecretKey};
use iroh_docs::{Author, protocol::Docs, ALPN as DOCS_ALPN};
use iroh::discovery::{dns::DnsDiscovery, pkarr::PkarrPublisher};
use iroh_blobs::{store::fs::FsStore as BlobStore, BlobsProtocol, ALPN as BLOBS_ALPN};
use iroh_gossip::{net::Gossip, ALPN as GOSSIP_ALPN};
use iroh::protocol::Router;
use iroh_n0des::Client as N0desClient;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;
use tokio::fs;
use futures::StreamExt;
use iroh_tickets::Ticket;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocTicketWrapper {
    pub ticket: String,
}

impl Ticket for DocTicketWrapper {
    const KIND: &'static str = "kinoteka-doc";

    fn to_bytes(&self) -> Vec<u8> {
        postcard::to_stdvec(&self).expect("postcard serialization failed")
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, iroh_tickets::ParseError> {
        let ticket: Self = postcard::from_bytes(bytes).map_err(|_e| iroh_tickets::ParseError::verification_failed("postcard deserialization failed"))?;
        Ok(ticket)
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Root {
    pub movies: Vec<MovieWrapper>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubtitleItemMetadata {
    pub id: Option<i64>,
    pub filename: String,
    pub download_link: Option<String>,
    pub date_uploaded: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MovieWrapper {
    pub title: String,
    pub year: Option<i64>,
    pub rating: Option<f64>,
    pub upload_date: Option<String>,
    pub date_uploaded: Option<String>,
    pub yts_data: Movie,
    pub subtitle_list: Option<Vec<SubtitleItemMetadata>>,
    pub is_featured: Option<bool>,
    // Compatibility fields
    pub subtitles: Option<Vec<String>>,
    pub subtitle_id: Option<i64>,
    pub download_link: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Movie {
    pub id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imdb_code: Option<String>,
    pub title: String,
    pub year: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rating: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description_full: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub large_cover_image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub torrents: Option<Vec<Torrent>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cast: Option<Vec<Cast>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Torrent {
    pub url: Option<String>,
    pub hash: String,
    pub quality: String,
    pub size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cast {
    pub name: String,
    pub character_name: Option<String>,
    pub url_small_image: Option<String>,
}

// --- Main ---

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: publisher <command> [args]");
        eprintln!("Commands:");
        eprintln!("  ingest <json_file>   Import movies from JSON to Iroh Doc");
        eprintln!("  join <ticket>        Join an existing Iroh Doc (Redundancy Node)");
        return Ok(());
    }

    let command = &args[1];
    match command.as_str() {
        "ingest" => {
            if args.len() < 3 {
                eprintln!("Usage: publisher ingest <json_file>");
                return Ok(());
            }
            let json_path = &args[2];
            ingest_movies(json_path).await?;
        }
        "join" => {
            if args.len() < 3 {
                eprintln!("Usage: publisher join <ticket>");
                return Ok(());
            }
            let ticket_str = &args[2];
            join_document(ticket_str).await?;
        }
        _ => eprintln!("Unknown command: {}", command),
    }

    Ok(())
}

async fn join_document(ticket_str: &str) -> Result<()> {
    log::info!("Starting Redundancy Node...");
    
    // Parse Ticket
    let ticket = iroh_docs::DocTicket::from_str(ticket_str).context("Invalid ticket format")?;

    // 1. Setup Data Directory
    // We use "publisher_data" by default, assuming this runs on a separate machine.
    let data_dir = PathBuf::from("publisher_data");
    fs::create_dir_all(&data_dir).await?;

    // 2. Load Secret Key
    let secret_key_path = data_dir.join("secret_key");
    let secret_key = if secret_key_path.exists() {
        let bytes = fs::read(&secret_key_path).await?;
        SecretKey::from_bytes(&bytes.try_into().unwrap())
    } else {
        let mut bytes = [0u8; 32];
        use rand::Rng; 
        rand::rng().fill(&mut bytes);
        let key = SecretKey::from_bytes(&bytes);
        fs::write(&secret_key_path, key.to_bytes()).await?;
        key
    };

    // 3. Init Iroh Node
    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .discovery(PkarrPublisher::n0_dns())
        .discovery(DnsDiscovery::n0_dns())
        .bind()
        .await?;
    
    log::info!("Node Peer ID: {}", endpoint.secret_key().public());

    let blobs_dir = data_dir.join("blobs");
    fs::create_dir_all(&blobs_dir).await?;
    let blob_store = BlobStore::load(&blobs_dir).await?;
    let gossip = Gossip::builder().spawn(endpoint.clone());
    
    let docs_dir = data_dir.join("docs");
    fs::create_dir_all(&docs_dir).await?;
    let docs = Docs::persistent(docs_dir)
        .spawn(endpoint.clone(), (*blob_store).clone(), gossip.clone())
        .await?;

    // 4. Start Router
    let router = Router::builder(endpoint.clone())
        .accept(BLOBS_ALPN, BlobsProtocol::new(&blob_store, Default::default()))
        .accept(GOSSIP_ALPN, gossip)
        .accept(DOCS_ALPN, docs.clone())
        .spawn();

    // 5. Join Document
    log::info!("Joining document: {}", ticket.capability.id());
    docs.import(ticket).await?;
    log::info!("✓ Joined document successfully! Node is now seeding.");

    // Keep running to serve data
    log::info!("Redundancy Node running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;

    Ok(())
}

async fn ingest_movies(json_path: &str) -> Result<()> {
    // 1. Setup Data Directory
    let data_dir = PathBuf::from("publisher_data");
    fs::create_dir_all(&data_dir).await?;

    // 2. Load Secret Key
    let secret_key_path = data_dir.join("secret_key");
    let secret_key = if secret_key_path.exists() {
        let bytes = fs::read(&secret_key_path).await?;
        SecretKey::from_bytes(&bytes.try_into().unwrap())
    } else {
        let mut bytes = [0u8; 32];
        use rand::Rng; 
        rand::rng().fill(&mut bytes);
        let key = SecretKey::from_bytes(&bytes);
        fs::write(&secret_key_path, key.to_bytes()).await?;
        key
    };

    // 3. Init Iroh Node
    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .discovery(PkarrPublisher::n0_dns())
        .discovery(DnsDiscovery::n0_dns())
        .bind()
        .await?;
    
    log::info!("Publisher ID: {}", endpoint.secret_key().public());

    // 3.1 Connect to n0des managed relays (if API secret is configured)
    let n0des_client = if std::env::var("N0DES_API_SECRET").is_ok() {
        match N0desClient::builder(&endpoint)
            .api_secret_from_env()
            .map_err(|e| anyhow::anyhow!("Failed to read N0DES_API_SECRET: {}", e))
            .and_then(|builder| Ok(builder))
        {
            Ok(builder) => {
                match builder.build().await {
                    Ok(client) => {
                        if let Ok(_) = client.ping().await {
                            log::info!("✓ Connected to n0des managed relays");
                        } else {
                            log::warn!("n0des ping failed, continuing without managed relays");
                        }
                        Some(client)
                    }
                    Err(e) => {
                        log::warn!("Failed to build n0des client: {}, continuing without", e);
                        None
                    }
                }
            }
            Err(e) => {
                log::warn!("n0des configuration error: {}, continuing without", e);
                None
            }
        }
    } else {
        log::info!("N0DES_API_SECRET not set, using default public relays");
        None
    };


    let blobs_dir = data_dir.join("blobs");
    fs::create_dir_all(&blobs_dir).await?;
    let blob_store = BlobStore::load(&blobs_dir).await?;
    let gossip = Gossip::builder().spawn(endpoint.clone());
    
    let docs_dir = data_dir.join("docs");
    fs::create_dir_all(&docs_dir).await?;
    let docs = Docs::persistent(docs_dir)
        .spawn(endpoint.clone(), (*blob_store).clone(), gossip.clone())
        .await?;

    // 4. Start Router
    let router = Router::builder(endpoint.clone())
        .accept(BLOBS_ALPN, BlobsProtocol::new(&blob_store, Default::default()))
        .accept(GOSSIP_ALPN, gossip)
        .accept(DOCS_ALPN, docs.clone())
        .spawn();

    // 5. Create/Load Document
    let author_path = data_dir.join("author_id");
    let author_id = if author_path.exists() {
        let bytes = fs::read(&author_path).await?;
        let author = Author::from_bytes(&bytes.try_into().unwrap());
        docs.author_import(author.clone()).await?;
        author.id()
    } else {
        let author_id = docs.author_create().await?;
        if let Some(author) = docs.author_export(author_id).await? {
            fs::write(&author_path, author.to_bytes()).await?;
        }
        author_id
    };

    // 5. Create/Load Document
    let doc = {
        let mut stream = docs.list().await?;
        let mut existing_doc = None;
        while let Some(item) = stream.next().await {
             if let Ok((id, kind)) = item {
                 // We assume the publisher manages only one document for simplicity
                 if matches!(kind, iroh_docs::CapabilityKind::Write) {
                     existing_doc = docs.open(id).await?;
                     if existing_doc.is_some() {
                         break;
                     }
                 }
             }
        }
        
        if let Some(doc) = existing_doc {
            log::info!("Loaded existing document: {}", doc.id());
            doc
        } else {
            log::info!("Creating new document...");
            docs.create().await?
        }
    };

    let ticket = iroh_docs::DocTicket {
        capability: iroh_docs::Capability::Read(doc.id()), 
        nodes: vec![endpoint.addr()],
    };
    
    let ticket_str = ticket.to_string();
    println!("\n=== DOCUMENT TICKET ===\n{}", ticket_str);
    fs::write(data_dir.join("ticket.txt"), ticket_str.clone()).await?;

    // 5.1 Publish Ticket to n0des for Discovery
    if let Some(client) = n0des_client {
        let wrapper = DocTicketWrapper { ticket: ticket_str };
        match client.publish_ticket("kinoteka_main_doc".to_string(), wrapper).await {
            Ok(_) => log::info!("✓ Document ticket published to n0des discovery (key: kinoteka_main_doc)"),
            Err(e) => log::warn!("Failed to publish ticket to n0des: {}", e),
        }
    }


    // 5.2 Publish Global Config to Doc
    let global_yts_url = "https://yts.lt"; // Default
    doc.set_bytes(author_id, "config:global_yts_url".to_string().into_bytes(), global_yts_url.to_string().into_bytes()).await?;
    log::info!("✓ Broadcasted global_yts_url: {} to Iroh Doc", global_yts_url);


    // 6. Ingest Data
    let json_content = fs::read_to_string(json_path).await.context("Failed to read JSON file")?;
    
    // Try to parse as the "movies" array format (Root)
    let movies: Vec<MovieWrapper> = if let Ok(root) = serde_json::from_str::<Root>(&json_content) {
        root.movies
    } else {
        // Try to parse as the map format (albanian_subs_v3.json)
        log::info!("Vec format failed, trying Map format...");
        let map: std::collections::HashMap<String, MovieWrapper> = serde_json::from_str(&json_content)
            .context("Failed to parse JSON as either Vec or Map structure. Verify the JSON schema.")?;
        map.into_values().collect()
    };

    log::info!("Ingesting {} movies...", movies.len());

    let mut count = 0;
    for wrapper in movies {
        if let Some(code) = &wrapper.yts_data.imdb_code {
            let key = format!("movie:{}", code);
            let value = serde_json::to_vec(&wrapper)?;
            
            doc.set_bytes(author_id, key.into_bytes(), value).await?;
            count += 1;
            if count % 100 == 0 {
                log::info!("Processed {} movies...", count);
            }
        }
    }

    log::info!("Successfully ingested {} movies!", count);

    // Keep running to serve data
    log::info!("Publisher running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;

    Ok(())
}
