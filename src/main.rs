use libkvstore::KVStore;

#[cfg(target_family = "unix")]
const USAGE: &'static str = "
Usage:
    kv_mem FILE get KEY
    kv_mem FILE delete KEY
    kv_mem FILE insert KEY VALUE
    kv_mem FILE update KEY VALUE
";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let fname = args.get(1).expect(&USAGE);
    let action = args.get(2).expect(&USAGE).as_ref();
    let key = args.get(3).expect(&USAGE).as_ref();
    let optional_value = args.get(4);

    let path = std::path::Path::new(&fname);
    let mut store = KVStore::open(path).expect("unable to open file");
    store.load().expect("unable to load data");

    match action {
        "get" => {
            match store.get(key).unwrap() {
                None => {
                    let key = String::from_utf8(key.to_vec()).expect("error converting from ut8");
                    eprintln!("{} not found", key);
                },
                Some(value) => println!("{:?}", value),
            }
        },
        "delete" => store.delete(key).unwrap(),
        "insert" => {
            let value = optional_value.expect(&USAGE).as_ref();
            store.insert(key, value).unwrap()
        },
        "update" => {
            let value = optional_value.expect(&USAGE).as_ref();
            store.update(key, value).unwrap()
        },
        _ => eprintln!("{}", &USAGE),
    }
}