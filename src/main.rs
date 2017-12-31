extern crate curl;
extern crate structopt;
#[macro_use]
extern crate structopt_derive;
#[macro_use] extern crate log;
extern crate env_logger;
#[macro_use]
extern crate tantivy;
extern crate whatlang;
extern crate serde_json;
extern crate chan;
extern crate itertools;
extern crate flate2;
extern crate rayon;
extern crate futures;

use futures::future::Future;
use flate2::read::MultiGzDecoder;
mod warc_reader;
use self::warc_reader::WARCReader;

use std::time::Duration;
use curl::easy;
use std::thread;
use std::mem;
use std::path::{Path, PathBuf};
use std::str;
use structopt::StructOpt;
use std::fs::{self, File};
use tantivy::{Index, IndexWriter, SegmentMeta, SegmentId};
use tantivy::schema::{Schema, SchemaBuilder, TEXT, STORED};
use std::io::{BufRead, BufReader};
use itertools::Itertools;

const WET_FILE: &'static str = "wet.txt";
const WAIT_AFTER_RETRY_SECONDS: u64 = 30u64;

#[derive(StructOpt, Debug)]
enum CliOption {
    #[structopt(name = "init")]
    Init {
        /// Needed parameter, the first on the command line.
        #[structopt(short = "i", long = "index", help = "Index directory")]
        index_directory: String,
        #[structopt(short = "f", long = "wet", help = "WET files list")]
        wet_list_file: String,
    },
    #[structopt(name = "run")]
    Run {
        /// Needed parameter, the first on the command line.
        #[structopt(short = "i", long = "index", help = "Index directory")]
        index_directory: String,
        #[structopt(long="root",
                    help="Root url to prepend to the WET files",
                    default_value="https://commoncrawl.s3.amazonaws.com/crawl-data/")]
        url_root: String,
    }
}

#[derive(Debug)]
struct WetFiles {
    files: Vec<String>
}

impl WetFiles {
    fn load(url_file: &Path) -> Result<WetFiles, String> {
        let file = File::open(url_file).map_err(|_| "Failed to open url file")?;
        let buf_reader = BufReader::new(file);
        let mut files = Vec::new();
        for line_res in buf_reader.lines() {
            let line = line_res.map_err(|_| String::from("Failed to read line. Probably not utf-8?"))?.trim().to_string();
            if !line.is_empty() {
                files.push(line);
            }
        }
        Ok(WetFiles {
            files
        })
    }

    fn len(&self) -> usize {
        self.files.len()
    }

    fn skip_to(&mut self, checkpoint: &str) {
        let pos = self.files.iter().position(|s| s == checkpoint).expect(&format!("Failed to find checkpoint {:?}", checkpoint));
        self.files = self.files[pos+1..].to_owned();
    }

    fn files(&self) -> &[String] {
        &self.files[..]
    }
}

fn schema() -> Schema {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_text_field("text", TEXT | STORED);
    schema_builder.add_text_field("url", STORED);
    schema_builder.build()
}

fn init(index_directory: &Path, wet_files: &Path) {
    if index_directory.exists() {
        panic!("Index directory already exists");
    }
    fs::create_dir(index_directory).expect("Failed to create index directory");
    Index::create(index_directory, schema()).expect("Failed to create the index");
    let dest_wet_file = index_directory.join(WET_FILE);
    fs::copy(wet_files, &dest_wet_file).expect("Failed to copy url file");
}

const CHUNK_SIZE: usize = 100;

pub struct WetData {
    pub wet_file: String,
    pub data: Vec<u8>,
}

impl WetData {
    fn data(&self) -> &[u8] {
        &self.data[..]
    }
}


#[derive(Default)]
struct DownloadToBuffer(pub Vec<u8>);

impl easy::Handler for DownloadToBuffer {
    fn write(&mut self, data: &[u8]) -> Result<usize, easy::WriteError> {
        self.0.extend_from_slice(data);
        Ok(data.len())
    }
}

impl DownloadToBuffer {
    fn purge(&mut self) -> Vec<u8> {
        mem::replace(&mut self.0, Vec::new())
    }
}

fn download(url: &str) -> Result<Vec<u8>, curl::Error> {
    let collector = DownloadToBuffer::default();
    let mut easy = easy::Easy2::new(collector);
    easy.get(true)?;
    easy.url(&url)?;
    easy.perform()?;
    assert_eq!(easy.response_code()?, 200);
    Ok(easy.get_mut().purge())
}

fn download_with_retry(url: &str, num_retries: usize) -> Result<Vec<u8>, curl::Error> {
    assert!(num_retries > 0);
    for _ in 0..(num_retries - 1)  {
        if let Ok(buffer) = download(url) {
            return Ok(buffer);
        } else {
            warn!("Failed, retrying!");
            thread::sleep(Duration::from_secs(WAIT_AFTER_RETRY_SECONDS))
        }
    }
    download(url)
}

fn download_wet(url_root: String, wet_files: WetFiles) -> chan::Receiver<WetData> {
    let (send, recv) = chan::sync(1);
    thread::spawn(move || {
        let num_files = wet_files.len();
        for (i, wet_file) in wet_files.files().iter().enumerate() {
            let url = format!("{}{}", url_root, wet_file);
            info!("DL {} / {}): {}", i, num_files, url);
            let wet_data: Vec<u8> = download_with_retry(&url, 10).expect("Download fail");
            send.send(WetData {
                wet_file: wet_file.clone(),
                data: wet_data,
            })
        }
    });
    recv
}

fn is_english(text: &str) -> bool {
    const SAMPLE_SIZE: usize = 500;
    let (start, stop) = {
        // detecting language is actually quite expensive. We only take 1000 byte in the middle.
        // because it is utf8 we need some logic to avoid cutting in the middle of a codepoint.
        if text.len() > SAMPLE_SIZE+8 {
            let target_start = (text.len() - SAMPLE_SIZE) / 2;
            let utf8_start = |target: usize| {
                (0..4)
                    .map(|i| target-i)
                    .filter(|i| { text.as_bytes()[*i] & 0b1100_0000 != 0b1000_0000 })
                    .next()
            };
            let start = utf8_start(target_start).unwrap_or(0);
            let stop = utf8_start(target_start + SAMPLE_SIZE).unwrap_or(text.len());
            (start, stop)
        } else {
            (0, text.len())
        }
    };
    let sample = &text[start..stop];
    if let Some(whatlang::Script::Latin) = whatlang::detect_script(sample) {
        let options = whatlang::Options::new().set_whitelist(vec![whatlang::Lang::Eng, whatlang::Lang::Spa, whatlang::Lang::Deu]);
        if let Some(info) = whatlang::detect_with_options(sample, &options) {
            return info.is_reliable();
        }
    }
    return false;
}

fn index_wet_file(wet_data: &WetData, schema: &Schema, index_writer: &mut IndexWriter) {
    info!("INDEXING {}. {} bytes Gzipped", wet_data.wet_file, wet_data.data().len());
    let mut cursor: &[u8] = wet_data.data();
    let decoder = MultiGzDecoder::new(&mut cursor);
    let warc_reader = WARCReader::new(decoder);
    let url_field = schema.get_field("url").expect("url field not in schema");
    let text_field = schema.get_field("text").expect("text field not in schema");

    let mut count = 0;
    for warc in warc_reader {
        if !is_english(&warc.text) {
            // we only index english content.
            continue;
        }
        count += 1;
        index_writer.add_document(doc!(
            url_field => warc.url,
            text_field => warc.text
        ));
    }
    info!("FINISHED INDEXING {}. {} docs", wet_data.wet_file, count);
}

fn resume_indexing(index_directory: &Path, url_root: &str) -> tantivy::Result<()> {
    let wet_files_file = index_directory.join(WET_FILE);
    let mut wet_files = WetFiles::load(&wet_files_file).expect("Failed to load url list file");
    let index = Index::open(index_directory)?;
    let schema = index.schema();
    let index_metas = index.load_metas()?;
    if let Some(checkpoint) = index_metas.payload {
        info!("Resuming at {:?}", checkpoint);
        wet_files.skip_to(&checkpoint);
    }
    {
        let mut index_writer = index.writer_with_num_threads(2, 2_000_000_000)?;
        let wet_queue = download_wet(url_root.to_string(), wet_files);

        for wet_files in wet_queue.into_iter().chunks(CHUNK_SIZE).into_iter() {
            let mut checkpoint = String::new();
            for wet_data in wet_files {
                index_wet_file(&wet_data, &schema, &mut index_writer);
                checkpoint = wet_data.wet_file.clone();
            }
            info!("PREPARE COMMIT: {}", checkpoint);
            let mut prepared_commit = index_writer.prepare_commit()?;
            prepared_commit.set_payload(&checkpoint);
            prepared_commit.commit()?;
            info!("COMMITTED: {}", checkpoint);
        }

        info!("Wait merging threads");
        index_writer.wait_merging_threads()?;
    }

    info!("Optimizing");
    loop {
        let mut segment_metas: Vec<SegmentMeta> = index.searchable_segment_metas()?;
        segment_metas.sort_by_key(|segment_meta| segment_meta.max_doc());
        let num_segments_to_merge = usize::min(8, segment_metas.len());
        if num_segments_to_merge <= 1 {
            break;
        }
        info!("Merging {} segments", num_segments_to_merge);
        let segment_ids: Vec<SegmentId> = segment_metas[..num_segments_to_merge]
            .iter()
            .map(|meta| meta.id())
            .collect();
        let mut index_writer = index
            .writer_with_num_threads(1, 10_000_000)?;

        index_writer
            .merge(&segment_ids)
            .wait()
            .expect("Merge failed");

        info!("Garbage collect irrelevant segments.");
        index_writer.garbage_collect_files()?;
    }
    Ok(())
}

fn main() {
    env_logger::init().unwrap();
    let cli_options = CliOption::from_args();
    match cli_options {
        CliOption::Init { index_directory, wet_list_file } => {
            let index_directory = PathBuf::from(index_directory);
            let url_file = PathBuf::from(wet_list_file);
            init(&index_directory, &url_file);
        }
        CliOption::Run { index_directory, url_root } => {
            let index_directory = PathBuf::from(index_directory);
            resume_indexing(&index_directory, &url_root).expect("Indexing failed");
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::fs::File;
    use flate2::read::MultiGzDecoder;
    use tantivy::Index;

    #[test]
    fn test_parse_warc() {
        let mut file = File::open("warc.wet.gz").unwrap();
        let mut data: Vec<u8> = vec![];
        file.read_to_end(&mut data).unwrap();
        let wet_data = WetData {
            wet_file: "coucou".to_string(),
            data,
        };
        let schema = super::schema();
        let index = tantivy::Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer_with_num_threads(1, 1_000_000_000).unwrap();
        super::index_wet_file(&wet_data, &schema, &mut index_writer);
    }


}

