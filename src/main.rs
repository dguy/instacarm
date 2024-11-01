use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use chrono::{Datelike, Local, Months, NaiveDate};

use serde_json::Value;
use sqlite::State;

use crate::model::{Relation, Relationships};
use itertools::Itertools;

mod model;

fn main() {
    let connection = sqlite::open("output/db.sqlite3").unwrap();
    connection.execute(
        "CREATE TABLE IF NOT EXISTS followers (
            name TEXT PRIMARY KEY,
            timestamp LONG
        )"
    ).unwrap();
    connection.execute(
        "CREATE TABLE IF NOT EXISTS followed (
            name TEXT PRIMARY KEY,
            timestamp LONG
        )"
    ).unwrap();

    let follower_str = fs::read_to_string("json/followers_1.json").unwrap();

    let followers = map_to_relations(serde_json::from_str(follower_str.as_str()).unwrap());

    let following_str = fs::read_to_string("json/following.json").unwrap();

    let x: Value = serde_json::from_str(following_str.as_str()).unwrap();
    let mut new_following = map_to_relations(x["relationships_following"].clone());

    maybe_insert_new(&connection, followers.clone(), "followers");
    maybe_insert_new(&connection, new_following.clone(), "followed");

    let mut statement = connection.prepare("SELECT name, timestamp FROM followed order by timestamp").unwrap();
    statement.next().unwrap();
    let mut following: Vec<Relation> = Vec::new();
    while let Ok(State::Row) = statement.next() {
        let name = statement.read::<String, _>("name").unwrap();
        let timestamp = statement.read::<i64, _>("timestamp").unwrap();
        following.push(Relation::new(name, timestamp));
    }

    let relationships = Relationships::new(new_following.clone(), followers.clone());

    let mut not_following_me = relationships.following_not_followers();
    not_following_me.sort();

    let  file = File::create("output/people_not_following_carma.csv").unwrap();
    let mut writer = BufWriter::new(file);

    for x in not_following_me {
        writer.write_fmt(format_args!("{}\n", x)).unwrap();
    }
    following.sort();

    write_to_file(&mut following, "output/all_the_people_carma_has_followed.csv");
    let mut data_grouped = Vec::new();
    for (key, chunk) in &followers.into_iter().chunk_by(|r| r.started_at()) {
        data_grouped.push((key, chunk.count()));
    }
    data_grouped.sort_by(|a, b| a.0.cmp(&b.0));
    let  file = File::create("output/new_followers_each_day.csv").unwrap();
    let mut new_followers_writer = BufWriter::new(file);

    for (key, value) in data_grouped {
        new_followers_writer.write_fmt(format_args!("{}, {}\n", key, value)).unwrap();
    }

    let mut next_date= NaiveDate::from_ymd_opt(2024, 8, 1).unwrap();
    let the_date = Local::now().naive_utc().date();
    let start_of_next_month = NaiveDate::from_ymd_opt(the_date.year(), the_date.month(), 1).unwrap().checked_add_months(Months::new(1)).unwrap();
    let file = File::create("output/follower_count_by_month.csv").unwrap();
    let mut new_followers_writer = BufWriter::new(file);
    let mut count_on_date = relationships.follower_count_on(next_date.checked_sub_months(Months::new(1)).unwrap());
    while next_date <= start_of_next_month {
        let this_month = relationships.follower_count_on(next_date);
        new_followers_writer.write_fmt(format_args!("{}, {}, {}\n", next_date, this_month, this_month - count_on_date)).unwrap();
        next_date = next_date.checked_add_months(Months::new(1)).unwrap();
        count_on_date  = this_month;

    }

}

fn write_to_file(following: &mut Vec<Relation>, path: &str) {
    let  file = File::create(path).unwrap();
    let mut writer = BufWriter::new(file);

    for x in following {
        writer.write_fmt(format_args!("{}\n", x)).unwrap();
    }
}

fn maybe_insert_new(connection: &sqlite::Connection, relations: Vec<Relation>, table: &str) {
    for x   in relations {
        let mut statement = connection.prepare(format!("INSERT INTO {} (name, timestamp) VALUES (:name, :timestamp)", table)).unwrap();
        statement.bind::<&[(_, sqlite::Value)]>(&[
            (":name", x.name().into()),
            (":timestamp", x.start_timestamp().into()),
        ][..]).unwrap();

        let _res = statement.next();
    }

}

fn map_to_relations(relations_json: Value) -> Vec<Relation> {
    let mut relations: Vec<Relation> = Vec::new();
    relations_json.as_array().unwrap().iter().for_each(|x| {
        for x in x["string_list_data"].as_array().unwrap().iter().map(|y| {
            Relation::new(y["value"].as_str().unwrap().to_string(), y["timestamp"].as_i64().unwrap())
        }) {
            relations.push(x);
        };
    });
    relations
}
