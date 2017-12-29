extern crate curl;
extern crate structopt;
#[macro_use]
extern crate structopt_derive;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate tantivy;
extern crate whatlang;
extern crate serde_json;
extern crate chan;
extern crate itertools;
extern crate warc_parser;
extern crate libflate;
extern crate nom;

use std::collections::BTreeMap;
use libflate::gzip::Decoder;
use curl::easy;
use std::thread;
use std::path::{Path, PathBuf};
use std::str;
use structopt::StructOpt;
use std::mem;
use std::fs::{self, File};
use tantivy::IndexWriter;
use tantivy::schema::{Schema, SchemaBuilder, TEXT, STORED};
use std::io::{Read, BufRead, BufReader};
use itertools::Itertools;
use nom::IResult;

const WET_FILE: &'static str = "wet.txt";


enum Status {
    COMPLETED,
    CHECKPOINT(String),
}


impl From<String> for Status {
    fn from(payload: String) -> Self {
        if payload == "COMPLETED" {
            Status::COMPLETED
        } else {
            Status::CHECKPOINT(payload)
        }
    }
}

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
        self.files = self.files[pos..].to_owned();
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
    tantivy::Index::create(index_directory, schema()).expect("Failed to create the index");
    let dest_wet_file = index_directory.join(WET_FILE);
    fs::copy(wet_files, &dest_wet_file).expect("Failed to copy url file");
}

const CHUNK_SIZE: usize = 10;

struct WetData {
    pub wet_file: String,
    data: Vec<u8>,
}

impl WetData {
    fn data(&self) -> &[u8] {
        &self.data[..]
    }
}


#[derive(Default)]
struct Collector(pub Vec<u8>);

impl easy::Handler for Collector {
    fn write(&mut self, data: &[u8]) -> Result<usize, easy::WriteError> {
        self.0.extend_from_slice(data);
        Ok(data.len())
    }

    fn progress(
        &mut self,
        dltotal: f64,
        dlnow: f64,
        ultotal: f64,
        ulnow: f64
    ) -> bool {
        //println!("{} / {}", dlnow, dltotal);
        true
    }
}


fn download_wet(url_root: String, wet_files: WetFiles) -> chan::Receiver<WetData> {
    let (send, recv) = chan::sync(3);
    thread::spawn(move || {
        for wet_file in wet_files.files() {
            let url = format!("{}{}", url_root, wet_file);
            info!("DL url: {}", url);
            let mut collector = Collector::default();
            let wet_data: Vec<u8> = {
                let mut easy = easy::Easy2::new(collector);
                easy.get(true).unwrap();
                easy.url(&url).unwrap();
                easy.progress(true);
                easy.perform().expect("Download fail");
                assert_eq!(easy.response_code().unwrap(), 200);
                easy.get_ref().0.clone()
            };
            let wet_data = WetData {
                wet_file: wet_file.clone(),
                data: wet_data,
            };
            send.send(wet_data)
        }
    });
    recv
}





struct WARCReader<R: Read> {
    header: BTreeMap<String, String>,
    buffer: Vec<u8>,
    reader: BufReader<R>
}

impl<R: Read> WARCReader<R> {

    fn new(r: R) -> WARCReader<R> {
        WARCReader {
            header: BTreeMap::new(),
            buffer: Vec::new(),
            reader: BufReader::new(r),
        }
    }

    fn url(&self) -> Option<&String> {
        self.header.get("WARC-Target-URI")
    }

    fn content(&self) -> &str {
        str::from_utf8(&self.buffer).expect("Content is not utf8")
    }

    fn read(&mut self) -> bool {
        self.header.clear();
        let mut line = String::new();
        while self.reader.read_line(&mut line).expect("io error") > 0 {
            if !line.trim().is_empty() {
                break;
            }
            line.clear();
        }
        if line.trim() != "WARC/1.0" {
            return false;
        }
        line.clear();
        let mut url: Option<String> = None;
        let mut content_len = 0;
        while self.reader.read_line(&mut line).expect("io error") > 0 {
            {
                let fields = line.trim().splitn(2, ":").collect::<Vec<&str>>();
                if fields.len() == 2 {
                    self.header.insert(fields[0].to_string(), fields[1].trim().to_string());
                } else {
                    break;
                }
            }
            line.clear();
        }
        let content_len_str = self.header.get("Content-Length").expect("Content length not found");
        let content_len: usize = content_len_str.parse().expect("Failed to parse content len");
        self.buffer.resize(content_len, 0u8);
        self.reader.read_exact(&mut self.buffer[..]).expect("Failed to read content");
        return true;
    }
}

//    if buf_reader.read_line(&mut line).expect("io error") {
//        while let Some(line) = buf_reader.read_line(&mut line).expect("failed to read line") {
//            let fields = line.split(":");ARC/1.0
////            let pos = line.position(|b| b==":");
////            if line.is_empty() {
////
////            }
//            line.clear();
//        }
//        Some(WARCHeader {
//            url: String::from(""),
//            content_len: 0
//        })
//    } else {
//        None
//    }


fn is_english(content: &str) -> bool {

}

const BUFFER_LEN: usize = 10_000_000;
fn index_wet_file(wet_data: &WetData, index_writer: &mut IndexWriter) {
    let mut cursor: &[u8] = wet_data.data();
    let mut decoder = Decoder::new(&mut cursor).expect("Opening gunzip for decompression");
    let mut warc_reader = WARCReader::new(decoder);
    while warc_reader.read() {
        if let Some(url) = warc_reader.url() {

        }
    }
}

fn resume_download(index_directory: &Path, url_root: &str) {
    let wet_files_file = index_directory.join(WET_FILE);
    let mut wet_files = WetFiles::load(&wet_files_file).expect("Failed to load url list file");
    let index = tantivy::Index::open(index_directory).expect("Failed to open the index");
    let index_metas = index.load_metas().expect("metas");
    if let Some(checkpoint) = index_metas.payload {
        let status = Status::from(checkpoint);
        match status {
            Status::COMPLETED => {
                println!("The current shard has already finished.");
                return;
            }
            Status::CHECKPOINT(checkpoint) => {
                info!("Resuming at {:?}", checkpoint);
                wet_files.skip_to(&checkpoint);
            }
        }
    }
    let mut index_writer = index.writer_with_num_threads(1, 1_000_000_000).expect("Failed to create index writer");
    let wet_queue = download_wet(url_root.to_string(), wet_files);

    for wet_files in wet_queue.into_iter().chunks(CHUNK_SIZE).into_iter() {
        let mut checkpoint = String::new();
        for wet_data in wet_files {
            info!("{}", wet_data.wet_file);
            index_wet_file(&wet_data, &mut index_writer);
            checkpoint = wet_data.wet_file.clone();
        }
        let mut prepared_commit = index_writer.prepare_commit().expect("Failed to prepare commit");
        prepared_commit.set_payload(&checkpoint);
        info!("COMMIT {}", checkpoint);
    }
//    println!("wet_files {:?}", wet_files);
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
            resume_download(&index_directory, &url_root);
        }
    }
}

mod tests {

    use super::*;

    const WARC_HEADER: &'static [u8] = include_bytes!("test.txt");

    #[test]
    fn test_parse_warc() {
        let mut warc_reader = WARCReader::new(WARC_HEADER);
        while warc_reader.read() {
            println!("url {:?}", warc_reader.url());
        }

//        let mut v = [0u8; 263];
//        buf.read_exact(&mut v);
//        println!("-");
//        println!("{}", str::from_utf8(&v).unwrap());
//        println!("-");
//        assert!(parse_warc_header(&mut buf).is_none());
    }


}

