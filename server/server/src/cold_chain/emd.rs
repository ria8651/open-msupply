use actix_web::web::Data;
use anyhow::{bail, Context, Result};
use chrono::NaiveDateTime;
use log::{info, warn};
use regex::Regex;
use repository::{TemperatureLogRow, TemperatureLogRowRepository};
use reqwest::{Client, Url};
use serde::{de::DeserializeOwned, Deserialize};
use service::{sensor::update::UpdateSensor, service_provider::ServiceProvider};
use std::time::Duration;
use util::{constants::SYSTEM_USER_ID, uuid::uuid};

const EMD_URL: &str = "http://192.168.1.248/json";
const SENSOR_ID: &str = "01953606-b1aa-7812-aca9-da61fb5a2e68";
const STORE_ID: &str = "8D967C2618BE4D78B3A6FAD6C1C8FF25";

#[derive(Debug, Deserialize)]
struct FileListing {
    entries: Vec<FileEntry>,
}

#[derive(Debug, Deserialize)]
struct FileEntry {
    name: String,
}

pub async fn cold_chain_emd_task(service_provider: Data<ServiceProvider>) {
    loop {
        if let Err(e) = requeset_current_data(&service_provider).await {
            warn!("Error requesting current data: {:?}", e);

            tokio::time::sleep(core::time::Duration::from_secs(5)).await;

            continue;
        }

        break;
    }

    loop {
        tokio::time::sleep(core::time::Duration::from_secs(60)).await;
    }
}

async fn requeset_current_data(service_provider: &ServiceProvider) -> Result<()> {
    let client = Client::new();

    // check when last data was imported
    let ctx = service_provider.context(STORE_ID.to_string(), SYSTEM_USER_ID.to_string())?;
    let sensor = service_provider
        .sensor_service
        .get_sensor(&ctx, SENSOR_ID.to_string())
        .anyhow()?;
    let last_download = sensor.sensor_row.last_connection_datetime;
    let should_use = |time: NaiveDateTime| last_download.is_some_and(|t| time > t);

    // find current data file
    let mut logger_start_time = None;
    let root_listing = get_endpoint::<FileListing>(&client, "")
        .await
        .context("Cannot get root listing")?;
    let mut data_files = vec![];
    for FileEntry { name, .. } in root_listing.entries {
        let Ok((_, file_name, relative_time, absolute_time)) = parse_logger_file_name(&name) else {
            continue;
        };

        if file_name == "CURRENT_DATA" {
            if !should_use(absolute_time) {
                info!("Skipping download as there is no new data");
                return Ok(());
            }

            logger_start_time = Some((absolute_time, relative_time));
            data_files.push(name);

            break;
        }
    }
    let Some((absolute_time, relative_time)) = logger_start_time else {
        bail!("No current data file found");
    };
    let logger_start_time = absolute_time - relative_time;

    // find history files
    let history_listing = get_endpoint::<FileListing>(&client, "DATA_HISTORY")
        .await
        .context("Cannot get history listing")?;
    for FileEntry { name, .. } in history_listing.entries {
        if name.starts_with(".") {
            continue;
        }

        // only import files that are newer than the last download
        let (_, file_name, relative_time, _) = parse_logger_file_name(&name)?;
        if file_name == "DATA" && should_use(logger_start_time + relative_time) {
            data_files.push(format!("DATA_HISTORY/{}", name));
        }
    }

    // add records to database
    let connection = service_provider.connection()?;
    for path in data_files {
        info!("Requesting data file {}...", path);
        let data_file = get_endpoint::<DataObject>(&client, &path)
            .await
            .context("Cannot get data file")?;

        info!("Processing data file {}...", path);
        let mut count = 0;
        for record in data_file.records {
            if let Some(temperature) = record.vacine_temperature {
                let relative = convert_duration(&record.relative_time)?;
                let timestamp = logger_start_time + relative;

                if !should_use(timestamp) {
                    continue;
                }

                let new_temperature_log = TemperatureLogRow {
                    id: uuid(),
                    store_id: "8D967C2618BE4D78B3A6FAD6C1C8FF25".to_string(),
                    sensor_id: "01953606-b1aa-7812-aca9-da61fb5a2e68".to_string(),
                    location_id: Some("80acf382-9d98-4f75-a8a6-f8cfc1cf916e".to_string()),
                    temperature: temperature as f64,
                    datetime: timestamp,
                    temperature_breach_id: None,
                };

                TemperatureLogRowRepository::new(&connection).upsert_one(&new_temperature_log)?;
                count += 1;
            }
        }
        info!("Imported {} records from {}", count, path);
    }

    // update last download time
    service_provider
        .sensor_service
        .update_sensor(
            &ctx,
            UpdateSensor {
                id: SENSOR_ID.to_string(),
                name: None,
                is_active: None,
                location_id: None,
                log_interval: None,
                battery_level: None,
                last_connection_datetime: Some(absolute_time),
            },
        )
        .anyhow()?;

    Ok(())
}

async fn get_endpoint<T: DeserializeOwned>(client: &Client, endpoint: &str) -> Result<T> {
    let url = Url::parse(&format!("{}/{}", EMD_URL, endpoint))?;
    let data_file = client.get(url).send().await?;
    if data_file.status().is_success() {
        Ok(data_file.json::<T>().await?)
    } else {
        bail!(
            "Got error {} from EMD: {:?}",
            data_file.status(),
            data_file.text().await?
        );
    }
}

// logger json file name format: <logger_id>_<file_name>_<relative_time>_<absolute_time>.json
// <file_name> can contain underscores
// <relative_time> is an ISO 8601 duration
fn parse_logger_file_name(name: &str) -> Result<(String, String, Duration, NaiveDateTime)> {
    let (logger_id, mut rest_of_file_name) = name
        .trim_end_matches(".json")
        .split_once("_")
        .context("Invalid logger file name")?;

    let absolute_time = rest_of_file_name
        .split("_")
        .last()
        .context("Invalid logger file name")?;

    rest_of_file_name = rest_of_file_name
        .trim_end_matches(absolute_time)
        .trim_end_matches("_");

    let relative_time = rest_of_file_name
        .split("_")
        .last()
        .context("Invalid logger file name")?;

    let file_name = rest_of_file_name
        .trim_end_matches(relative_time)
        .trim_end_matches("_");

    Ok((
        logger_id.to_string(),
        file_name.to_string(),
        convert_duration(relative_time)?,
        convert_absolute(absolute_time)?,
    ))
}

// ISO 8601 date time parser
fn convert_absolute(absolute_time: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(absolute_time, "%Y%m%dT%H%M%SZ").context("Invalid absolute time")
}

// ISO 8601 duration parser
fn convert_duration(duration: &str) -> Result<Duration> {
    let duration_regex = Regex::new("P(?:(?<years>[.,\\d]+)Y)?(?:(?<months>[.,\\d]+)M)?(?:(?<weeks>[.,\\d]+)W)?(?:(?<days>[.,\\d]+)D)?(?:T(?:(?<hours>[.,\\d]+)H)?(?:(?<minutes>[.,\\d]+)M)?(?:(?<seconds>[.,\\d]+)S)?)?").unwrap();
    let duration = duration_regex
        .captures(duration)
        .context("Invalid duration")?;

    let p = |m: regex::Match<'_>| m.as_str().parse::<f32>();
    let years = duration.name("years").map_or(Ok(0.0), p)?;
    let months = duration.name("months").map_or(Ok(0.0), p)?;
    let weeks = duration.name("weeks").map_or(Ok(0.0), p)?;
    let days = duration.name("days").map_or(Ok(0.0), p)?;
    let hours = duration.name("hours").map_or(Ok(0.0), p)?;
    let minutes = duration.name("minutes").map_or(Ok(0.0), p)?;
    let seconds = duration.name("seconds").map_or(Ok(0.0), p)?;

    // TODO: The ISO 8601 duration requires context to know how many days are in a month and year,
    // what are we supposed to do with only a duration?
    Ok(Duration::from_secs(
        (years * 365.0 * 24.0 * 60.0 * 60.0
            + months * 30.0 * 24.0 * 60.0 * 60.0
            + weeks * 7.0 * 24.0 * 60.0 * 60.0
            + days * 24.0 * 60.0 * 60.0
            + hours * 60.0 * 60.0
            + minutes * 60.0
            + seconds) as u64,
    ))
}

// based off of the WHO/PQS/E006/DS01 json format
#[derive(Debug, Deserialize)]
struct DataObject {
    records: Vec<DataRecord>,
}

#[derive(Debug, Deserialize)]
struct DataRecord {
    #[serde(rename = "RELT")]
    relative_time: String,
    // #[serde(rename = "RTCW")]
    // brownout_recovery_time: String,
    #[serde(rename = "TVC")]
    vacine_temperature: Option<f32>,
    // #[serde(rename = "ALRM")]
    // alarm: Option<Alarm>,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum Alarm {
    #[serde(rename = "HEAT")]
    Heat,
    #[serde(rename = "FRZE")]
    Freeze,
    #[serde(rename = "DOOR")]
    Door,
    #[serde(rename = "POWR")]
    Power,
    #[serde(rename = "DCNT")]
    Disconnected,
}

trait ToAnyhow<A> {
    fn anyhow(self) -> Result<A>;
}

impl<A, T: std::fmt::Debug> ToAnyhow<A> for Result<A, T> {
    fn anyhow(self) -> Result<A> {
        self.map_err(|e| anyhow::anyhow!("{:?}", e))
    }
}
