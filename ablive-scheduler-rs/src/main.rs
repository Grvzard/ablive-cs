mod settings;

use bson::{bson, doc, oid::ObjectId, Bson};
use chrono::prelude::*;
use futures::stream::TryStreamExt;
use serde::{self, Deserialize, Serialize};
use settings::Settings;
use std::{
    collections::{HashSet, VecDeque},
    hash::Hash,
};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
struct Room(i64, i64); // (liver_uid, room_id)

impl From<Room> for Bson {
    fn from(room: Room) -> Self {
        bson!([room.0, room.1])
    }
}

#[derive(Debug)]
struct WeightedRoom {
    weight: i64,
    room: Room,
}

impl From<WeightedRoom> for Room {
    fn from(room: WeightedRoom) -> Self {
        room.room
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RoomStateDoc {
    _id: ObjectId,
    uid: i64,
    roomid: i64,
    parent_name: String,
    watched_num: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct RoomsWorkerDoc {
    _id: ObjectId,
    checked: i32,
    length: i32,
    rooms: Vec<Room>,
}

#[derive(Debug, Clone)]
struct RoomsWorker {
    _id: ObjectId,
    length: i32,
    rooms: HashSet<Room>,
}

impl From<RoomsWorkerDoc> for RoomsWorker {
    fn from(doc: RoomsWorkerDoc) -> Self {
        RoomsWorker {
            _id: doc._id,
            length: doc.length,
            rooms: doc.rooms.into_iter().collect(),
        }
    }
}

impl From<RoomsWorker> for RoomsWorkerDoc {
    fn from(worker: RoomsWorker) -> Self {
        RoomsWorkerDoc {
            _id: worker._id,
            checked: 0,
            length: worker.length,
            rooms: worker.rooms.into_iter().collect(),
        }
    }
}

impl RoomsWorker {
    fn rooms_append(&mut self, room: Room) {
        if self.rooms.insert(room) {
            self.length += 1;
        }
    }

    #[allow(dead_code)]
    fn rooms_remove(&mut self, room: Room) {
        if self.rooms.remove(&room) {
            self.length -= 1;
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SettingsColl<T> {
    key: String,
    value: T,
}

type LiveroomsColl = SettingsColl<Vec<Room>>;

#[allow(dead_code)]
struct HeartbeatColl {
    module: String,
    hb_ts: i64,
    hb: chrono::DateTime<Utc>,
}

async fn fill_rooms_pool() -> Vec<Room> {
    let settings = Settings::new().unwrap();
    let area_weight = settings.area_weight;

    let mg_cli = mongodb::Client::with_uri_str(&settings.mongo_liveroom)
        .await
        .unwrap();

    let mut weighted_rooms: Vec<WeightedRoom> = vec![];

    let blive_rooms: Vec<Room> = mg_cli
        .database("bili_liveroom")
        .collection::<LiveroomsColl>("settings")
        .find_one(doc! { "key": "blive_rooms" }, None)
        .await
        .unwrap()
        .unwrap()
        .value;
    for room in blive_rooms {
        weighted_rooms.push(WeightedRoom {
            weight: 1000000,
            room: Room(room.0, room.1),
        })
    }

    let mut ablive_rooms = mg_cli
        .database("bili_liveroom")
        .collection::<RoomStateDoc>("rooms_state")
        .find(doc! {}, None)
        .await
        .unwrap();
    while let Some(doc) = ablive_rooms.try_next().await.unwrap() {
        weighted_rooms.push(WeightedRoom {
            weight: *area_weight.get(&doc.parent_name).unwrap() + doc.watched_num as i64,
            room: Room(doc.uid, doc.roomid),
        })
    }

    weighted_rooms.sort_unstable_by_key(|e| -e.weight);
    // println!("{:?}", weighted_rooms);

    weighted_rooms.into_iter().map(|room| room.into()).collect()
}

async fn do_schedule() {
    let settings = Settings::new().unwrap();
    let mg_cli = mongodb::Client::with_uri_str(&settings.mongo_worker)
        .await
        .unwrap();
    let mgdb = mg_cli.database("bili_liveroom");

    let mut workers: Vec<RoomsWorker> = fetch_workers(&mgdb).await;

    let cap = settings.rooms_per_worker * workers.len() as i32;
    let rooms_pool: HashSet<Room> = fill_rooms_pool()
        .await
        .into_iter()
        .take(cap as usize)
        .collect();
    adjust_workers(&mut workers, rooms_pool, settings.rooms_per_worker).await;
    update_workers(&mgdb, &workers).await;
    update_hb(&mgdb).await;
}

async fn fetch_workers(mgdb: &mongodb::Database) -> Vec<RoomsWorker> {
    let mut workers: Vec<RoomsWorker> = vec![];
    let mut cursor = mgdb
        .collection::<RoomsWorkerDoc>("workers")
        .find(doc! {}, None)
        .await
        .unwrap();
    while let Some(doc) = cursor.try_next().await.unwrap() {
        workers.push(doc.into());
    }
    workers
}

async fn adjust_workers(
    workers: &mut [RoomsWorker],
    mut rooms_pool: HashSet<Room>,
    rooms_per_worker: i32,
) {
    for worker in workers.iter_mut() {
        let rooms: HashSet<Room> = worker
            .rooms
            .iter()
            .filter(|&room| rooms_pool.remove(room))
            .cloned()
            .collect();
        *worker = RoomsWorker {
            _id: worker._id,
            length: rooms.len() as i32,
            rooms,
        };
        dbg!("{}", worker.length);
    }
    let mut workers_nonfull: VecDeque<&mut RoomsWorker> = workers
        .iter_mut()
        .filter(|worker| worker.length < rooms_per_worker)
        .collect();
    for room in rooms_pool.into_iter() {
        if let Some(worker) = workers_nonfull.front_mut() {
            worker.rooms_append(room);
            if worker.length >= rooms_per_worker {
                workers_nonfull.pop_front();
            }
        } else {
            break;
        }
    }
}

async fn update_workers(mgdb: &mongodb::Database, workers: &[RoomsWorker]) {
    for worker in workers.iter() {
        let doc = RoomsWorkerDoc::from(worker.clone());
        mgdb.collection::<RoomsWorkerDoc>("workers")
            .update_one(
                doc! { "_id": &doc._id },
                doc! { "$set": {"rooms": &doc.rooms, "length": &doc.length, "checked": 0} },
                None,
            )
            .await
            .unwrap();
    }
}

async fn update_hb(mgdb: &mongodb::Database) {
    let now = Utc::now();
    let ts: i64 = now.timestamp();
    mgdb.collection::<HeartbeatColl>("heartbeat")
        .update_one(
            doc! {"module": "ablive-scheduler"},
            doc! {"$set": {"hb_ts": ts, "hb": now}},
            Some(
                mongodb::options::UpdateOptions::builder()
                    .upsert(true)
                    .build(),
            ),
        )
        .await
        .unwrap();
}

#[tokio::main]
async fn main() {
    do_schedule().await;
}
