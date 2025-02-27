use actix_web::{web::Data, HttpRequest, HttpResponse};
use anyhow::{bail, Context, Result};
use chrono::NaiveDateTime;
use log::{error, info, warn};
use regex::Regex;
use repository::{SensorType, TemperatureLogRow, TemperatureLogRowRepository};
use reqwest::{Client, Url};
use serde::{de::DeserializeOwned, Deserialize};
use service::{
    sensor::{insert::InsertSensor, update::UpdateSensor},
    service_provider::ServiceProvider,
    SingleRecordError,
};
use std::time::Duration;
use util::{constants::SYSTEM_USER_ID, uuid::uuid};

const EMD_URL: &str = "http://192.168.1.248/json";
const STORE_ID: &str = "8D967C2618BE4D78B3A6FAD6C1C8FF25";

#[derive(Debug, Deserialize)]
struct FileListing {
    entries: Vec<FileEntry>,
}

#[derive(Debug, Deserialize)]
struct FileEntry {
    name: String,
}

pub async fn emd_ready(
    _request: HttpRequest,
    _sensor_id: String,
    service_provider: Data<ServiceProvider>,
) -> HttpResponse {
    // no authentication for now

    if let Err(e) = requeset_current_data(&service_provider).await {
        warn!("Error requesting current data: {:?}", e);
    }

    HttpResponse::Ok().finish()
}

pub async fn cold_chain_emd_task(_service_provider: Data<ServiceProvider>) {
    // loop {
    //     if let Err(e) = requeset_current_data(&service_provider).await {
    //         warn!("Error requesting current data: {:?}", e);

    //         tokio::time::sleep(core::time::Duration::from_secs(5)).await;

    //         continue;
    //     }

    //     break;
    // }

    loop {
        tokio::time::sleep(core::time::Duration::from_secs(60)).await;
    }
}

async fn requeset_current_data(service_provider: &ServiceProvider) -> Result<()> {
    let client = Client::new();

    // find current data file
    let mut logger_start_time = None;
    let root_listing = get_endpoint::<FileListing>(&client, "")
        .await
        .context("Cannot get root listing")?;
    let mut data_files = vec![];
    let mut sync_file = None;
    for FileEntry { name, .. } in root_listing.entries {
        let Ok((_, file_name, relative_time, absolute_time)) = parse_logger_file_name(&name) else {
            continue;
        };

        if file_name == "SYNC" {
            sync_file = Some(name);
            logger_start_time = Some((absolute_time, relative_time));
        } else if file_name == "CURRENT_DATA" {
            data_files.push((name, relative_time));
        } else if file_name == "DATA" {
            data_files.push((name, relative_time));
        }
    }
    let Some((absolute_time, relative_time)) = logger_start_time else {
        bail!("No current data file found");
    };
    let logger_start_time = absolute_time - relative_time;

    // read sync file
    let Some(sync_file_path) = sync_file else {
        bail!("No sync file found");
    };
    let sync_file = get_endpoint::<DataObject>(&client, &sync_file_path)
        .await
        .context("Cannot get sync file")?;
    let sensor_id = sync_file
        .sensor_id()
        .context("Sync file has no serial number")?;

    // check when last data was imported
    let ctx = service_provider.context(STORE_ID.to_string(), SYSTEM_USER_ID.to_string())?;
    let sensor_service = &service_provider.sensor_service;
    let sensor = match sensor_service.get_sensor(&ctx, sensor_id.clone()) {
        Ok(sensor) => sensor,
        Err(SingleRecordError::NotFound(_)) => {
            info!("Sensor {} not found, creating...", &sensor_id);

            let new_sensor = InsertSensor {
                id: sensor_id.clone(),
                serial: sensor_id.clone(),
                name: sync_file.name(),
                is_active: Some(true),
                log_interval: Some(300),
                battery_level: None,
                r#type: SensorType::Berlinger, // TODO: add new sensor type
            };
            sensor_service.insert_sensor(&ctx, new_sensor).anyhow()?
        }
        Err(e) => bail!("Cannot get sensor: {:?}", e),
    };

    // check if we should import data
    let last_download = sensor.sensor_row.last_connection_datetime;
    let should_use = |time: NaiveDateTime| last_download.is_none_or(|t| time > t);

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
            data_files.push((format!("DATA_HISTORY/{}", name), relative_time));
        }
    }

    // add records to database
    let connection = service_provider.connection()?;
    for (path, file_relative_time) in data_files {
        if !should_use(logger_start_time + file_relative_time) {
            info!("Skipping data file {}...", path);
            continue;
        }

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
                    store_id: STORE_ID.to_string(),
                    sensor_id: sensor_id.clone(),
                    location_id: None,
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
                id: sensor_id,
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
    #[serde(rename = "AMOD")]
    appliance_model: Option<String>,
    #[serde(rename = "ASER")]
    appliance_serial: Option<String>,
    #[serde(rename = "LMOD")]
    logger_model: Option<String>,
    #[serde(rename = "LSER")]
    logger_serial: Option<String>,
    records: Vec<DataRecord>,
}

impl DataObject {
    fn name(&self) -> Option<String> {
        match (&self.appliance_model, &self.logger_model) {
            (Some(appliance), Some(logger)) => Some(format!("{}-{}", appliance, logger)),
            (Some(appliance), None) => Some(appliance.clone()),
            (None, Some(logger)) => Some(logger.clone()),
            (None, None) => None,
        }
    }

    fn sensor_id(&self) -> Option<String> {
        match (&self.appliance_serial, &self.logger_serial) {
            (Some(appliance), Some(logger)) => Some(format!("{}-{}", appliance, logger)),
            (Some(appliance), None) => Some(appliance.clone()),
            (None, Some(logger)) => Some(logger.clone()),
            (None, None) => None,
        }
    }
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
