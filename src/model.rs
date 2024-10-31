use std::fmt;
use std::fmt::Display;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

#[derive(Debug, Clone, Eq,  Hash, Ord)]
pub struct Relation {
    name: String,
    start_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Relationships {
    following: Vec<Relation>,
    followers: Vec<Relation>,
}

impl Relation {
    pub fn new(name: String, start_timestamp: i64) -> Self {
        Self {
            name,
            start_timestamp,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn start_timestamp(&self) -> i64 {
        self.start_timestamp
    }

    pub fn started_at(&self) -> NaiveDate {
        DateTime::from_timestamp(self.start_timestamp, 0).unwrap().date_naive()
    }
}


impl PartialEq for Relation {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}


impl Display for Relation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dt = DateTime::from_timestamp(self.start_timestamp, 0).unwrap();

        let dt_str = dt.format("%Y-%m-%d %H:%M:%S");
        write!(f, "{}", format!("{}, {}", self.name, dt_str))
    }
}

impl PartialOrd for Relation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.name.cmp(&other.name))
    }
}

impl Relationships {
    pub fn new(following: Vec<Relation>, followers: Vec<Relation>) -> Self {
        Self {
            following,
            followers,
        }
    }

    pub fn following(&self) -> &Vec<Relation> {
        &self.following
    }

    pub fn followers(&self) -> &Vec<Relation> {
        &self.followers
    }

    pub fn following_not_followers(&self) -> Vec<&Relation> {
        self.following
            .iter()
            .filter(|x| !self.followers.contains(x))
            .collect()
    }

    pub fn follower_count_on(&self, date: NaiveDate) -> usize {
        self.followers
            .iter()
            .filter(|x| x.started_at() <= date)
            .count()
    }
}