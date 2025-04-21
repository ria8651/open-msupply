use actix_web::{
    web::{Bytes, Data},
    HttpRequest, HttpResponse,
};
use anyhow::{bail, Context, Result};
use chrono::NaiveDateTime;
use log::{info, warn};
use regex::Regex;
use repository::{
    DatetimeFilter, EqualFilter, Sensor, SensorFilter, SensorType, TemperatureLog,
    TemperatureLogFilter, TemperatureLogRow, TemperatureLogRowRepository,
};
use reqwest::{Client, ClientBuilder, Url};
use serde::{de::DeserializeOwned, Deserialize};
use service::{
    cold_chain::query_temperature_log::get_temperature_logs,
    sensor::{insert::InsertSensor, update::UpdateSensor},
    service_provider::ServiceProvider,
    ListResult, SingleRecordError,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::{Receiver, Sender};
use util::{constants::SYSTEM_USER_ID, uuid::uuid};

const EMD_URL: &str = "http://192.168.1.248/json";
const STORE_ID: &str = "8D967C2618BE4D78B3A6FAD6C1C8FF25";

pub fn spawn_cold_chain_task(
    service_provider: Data<ServiceProvider>,
) -> (tokio::task::JoinHandle<()>, Sender<ProcessingFile>) {
    let (sender, receiver) = tokio::sync::mpsc::channel(100);

    let handle = tokio::spawn(cold_chain_emd_task(receiver, service_provider));

    (handle, sender)
}

/// periodic task to request current data from EMD
/// currently using the emd_ready notification as a trigger instead - should use both
pub async fn cold_chain_emd_task(
    mut receiver: Receiver<ProcessingFile>,
    service_provider: Data<ServiceProvider>,
) {
    loop {
        let processing_file = receiver.recv().await.unwrap();
        info!("{}: processing data file...", processing_file.path);
        let count = match process_file(
            processing_file.data,
            &processing_file.sensor,
            processing_file.logger_start_time,
            &service_provider,
        ) {
            Ok(count) => count,
            Err(e) => {
                warn!(
                    "Failed to process data file {}: {:?}",
                    processing_file.path, e
                );
                continue;
            }
        };
        info!("{}: imported {} records", processing_file.path, count);
    }
}

/// EMD notification that the logger is ready for the server to download data
/// * Only sent when the EMD cold chain is set to Pull mode
pub async fn emd_ready(
    processing_file: Data<Sender<ProcessingFile>>,
    service_provider: Data<ServiceProvider>,
) -> HttpResponse {
    // no authentication for now

    if let Err(e) = requeset_current_data((*processing_file).clone(), &service_provider).await {
        warn!("Error requesting current data: {:?}", e);
    }

    HttpResponse::Ok().finish()
}

/// EMD upload endpoint for the logger to send data files
/// * Only sent when the EMD cold chain is set to Push mode
pub async fn emd_upload(
    request: HttpRequest,
    file: Bytes,
    processing_file: Data<Sender<ProcessingFile>>,
    service_provider: Data<ServiceProvider>,
) -> HttpResponse {
    // no authentication for now

    // parse file name
    let path = request.match_info().get("path").unwrap();
    let (_, full_file_name) = path.rsplit_once("/").unwrap_or(("", path));
    let Ok((logger_id, file_name, relative_time, absolute_time)) =
        parse_logger_file_name(full_file_name)
    else {
        return HttpResponse::BadRequest().finish();
    };

    // only use data files
    if file_name != "CURRENT_DATA" && file_name != "DATA" {
        return HttpResponse::Forbidden().finish();
    }

    info!("received file: {:?}", full_file_name);

    // get relative time from header
    let Some(logger_current_relative_time) = request
        .headers()
        .get("X-LoggerRelativeTime")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| convert_duration(s).ok())
    else {
        info!(
            "{}: bad request: missing X-LoggerRelativeTime header",
            full_file_name
        );
        return HttpResponse::BadRequest().finish();
    };
    let logger_start_time = absolute_time - logger_current_relative_time;
    let file_time = logger_start_time + relative_time;

    // lookup sensor
    let ctx = service_provider
        .context(STORE_ID.to_string(), SYSTEM_USER_ID.to_string())
        .unwrap();
    let sensor_service = &service_provider.sensor_service;
    let sensor = sensor_service.get_sensor(&ctx, logger_id.clone()).ok();

    // check if sensor exists
    let sensor = match sensor {
        Some(sensor) => {
            // check if record exists in the database
            let logs = match check_database_for_temp_logs(&sensor, file_time, &service_provider) {
                Ok(logs) => logs,
                Err(e) => {
                    warn!("Failed to check database for temperature logs: {:?}", e);
                    return HttpResponse::InternalServerError().finish();
                }
            };

            if logs.count > 0 {
                info!("{}: data already exists in the database", full_file_name);
                return HttpResponse::Conflict().finish();
            }

            sensor
        }
        None => {
            // create sensor
            info!("Sensor {} not found, creating...", &logger_id);

            let new_sensor = InsertSensor {
                id: logger_id.clone(),
                serial: logger_id.clone(),
                name: None,
                is_active: Some(true),
                log_interval: Some(300),
                battery_level: None,
                r#type: SensorType::Berlinger, // TODO: add new sensor type
            };
            let sensor = match sensor_service.insert_sensor(&ctx, new_sensor) {
                Ok(sensor) => sensor,
                Err(e) => {
                    info!("Failed to create sensor: {:?}", e);
                    return HttpResponse::InternalServerError().finish();
                }
            };
            info!("Sensor {} created", sensor.sensor_row.id);

            sensor
        }
    };

    // let mut chunks = 0;
    // while let Some(chunk) = file.next().await {

    //     chunks += 1;
    // }

    // // if message has no body send a continue
    // if chunks == 0 {
    //     info!("{}: continue", full_file_name);
    //     return HttpResponse::Continue().finish();
    // }

    // if message has no body send a continue
    if file.len() == 0 {
        info!("{}: continue", full_file_name);
        return HttpResponse::Continue().finish();
    }

    // process file
    let data_file: DataObject = match serde_json::from_slice(&file) {
        Ok(data_file) => data_file,
        Err(e) => {
            warn!("Failed to parse data file {}: {:?}", path, e);
            return HttpResponse::BadRequest().finish();
        }
    };
    info!("{}: sending file for processing...", full_file_name);
    if let Err(e) = processing_file
        .send(ProcessingFile {
            path: full_file_name.to_string(),
            data: data_file,
            sensor: sensor,
            logger_start_time: logger_start_time,
        })
        .await
    {
        warn!("Failed to send data file for processing: {:?}", e);
        return HttpResponse::InternalServerError().finish();
    }
    // let count = match process_file(data_file, &sensor, logger_start_time, &service_provider) {
    //     Ok(count) => count,
    //     Err(e) => {
    //         warn!("Failed to process data file {}: {:?}", path, e);
    //         return HttpResponse::InternalServerError().finish();
    //     }
    // };
    // info!("{}: imported {} records", full_file_name, count);

    HttpResponse::Ok().finish()
}

/// Based off of the WHO/PQS/E006/DS01 data layout
/// * Looks for a SYNC file to determine the logger start time
/// * Looks for CURRENT_DATA and DATA files to import
/// * Looks through the DATA_HISTORY directory for DATA files to import
async fn requeset_current_data(
    processing_file: Arc<Sender<ProcessingFile>>,
    service_provider: &ServiceProvider,
) -> Result<()> {
    let client = ClientBuilder::new()
        .timeout(Duration::from_secs(30))
        .build()?;

    // find current data file
    let mut logger_start_time = None;
    let root_listing = get_endpoint::<FileListing>(&client, "")
        .await
        .context("Cannot get root listing")?;
    let mut data_files = vec![];
    let mut sync_file = None;
    for FileEntry { name, .. } in root_listing.entries {
        let Ok((logger_id, file_name, relative_time, absolute_time)) =
            parse_logger_file_name(&name)
        else {
            continue;
        };

        if file_name == "SYNC" {
            sync_file = Some((logger_id, name));
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
    let Some((logger_id, sync_file_path)) = sync_file else {
        bail!("No sync file found");
    };
    let sync_file = get_endpoint::<DataObject>(&client, &sync_file_path)
        .await
        .context("Cannot get sync file")?;

    // find the sensor
    let ctx = service_provider.context(STORE_ID.to_string(), SYSTEM_USER_ID.to_string())?;
    let sensor_service = &service_provider.sensor_service;
    let sensor = match sensor_service.get_sensor(&ctx, logger_id.clone()) {
        Ok(sensor) => sensor,
        Err(SingleRecordError::NotFound(_)) => {
            info!("Sensor {} not found, creating...", &logger_id);

            let new_sensor = InsertSensor {
                id: logger_id.clone(),
                serial: sync_file.sensor_id().unwrap(),
                name: sync_file.name(),
                is_active: Some(true),
                log_interval: Some(300),
                battery_level: None,
                r#type: SensorType::Berlinger, // TODO: add new sensor type
            };
            sensor_service
                .insert_sensor(&ctx, new_sensor)
                .anyhow("Failed to insert sensor")?
        }
        Err(e) => bail!("Cannot get sensor: {:?}", e),
    };

    // find history files
    let history_listing = get_endpoint::<FileListing>(&client, "DATA_HISTORY")
        .await
        .context("Cannot get history listing")?;
    for FileEntry { name, .. } in history_listing.entries {
        if name.starts_with(".") {
            continue;
        }

        let (_, file_name, relative_time, _) = parse_logger_file_name(&name)?;
        if file_name == "DATA" {
            data_files.push((format!("DATA_HISTORY/{}", name), relative_time));
        }
    }

    // add records to database
    for (path, file_relative_time) in data_files {
        // check if record exists in the database
        let file_time = logger_start_time + file_relative_time;
        let logs = check_database_for_temp_logs(&sensor, file_time, service_provider)?;
        if logs.count > 0 {
            info!(
                "Data file {} already exists in the database, checked {}",
                path, file_time
            );
            continue;
        }

        // if not, download the file
        info!("Requesting data file {}...", path);
        let data_file = get_endpoint::<DataObject>(&client, &path)
            .await
            .context("Cannot get data file")?;

        // info!("Processing data file {}...", path);
        // let count = process_file(data_file, &sensor, logger_start_time, service_provider)?;
        // info!("Imported {} records from {}", count, path);

        // send file for processing
        processing_file
            .send(ProcessingFile {
                path,
                data: data_file,
                sensor: sensor.clone(),
                logger_start_time,
            })
            .await
            .context("Failed to send data file for processing")?;
    }

    // update last download time
    service_provider
        .sensor_service
        .update_sensor(
            &ctx,
            UpdateSensor {
                id: logger_id.clone(),
                name: None,
                is_active: None,
                location_id: None,
                log_interval: None,
                battery_level: None,
                last_connection_datetime: Some(absolute_time),
            },
        )
        .anyhow("Failed to update sensor")?;

    Ok(())
}

pub struct ProcessingFile {
    path: String,
    data: DataObject,
    sensor: Sensor,
    logger_start_time: NaiveDateTime,
}

fn process_file(
    data_file: DataObject,
    sensor: &Sensor,
    logger_start_time: NaiveDateTime,
    service_provider: &ServiceProvider,
) -> Result<usize> {
    let mut count = 0;
    for record in data_file.records {
        if let Some(temperature) = record.vacine_temperature {
            let record_time = logger_start_time + convert_duration(&record.relative_time)?;

            // TODO: this is very slow
            let logs = check_database_for_temp_logs(sensor, record_time, service_provider)?;
            if logs.count == 0 {
                let new_temperature_log = TemperatureLogRow {
                    id: uuid(),
                    temperature: temperature as f64,
                    sensor_id: sensor.sensor_row.id.clone(),
                    location_id: None,
                    store_id: sensor.sensor_row.store_id.clone(),
                    datetime: record_time,
                    temperature_breach_id: None,
                };

                let connection = service_provider.connection()?;
                TemperatureLogRowRepository::new(&connection).upsert_one(&new_temperature_log)?;
                count += 1;

                // let ctx =
                //     service_provider.context(STORE_ID.to_string(), SYSTEM_USER_ID.to_string())?;
                // insert_temperature_log(&ctx, new_temperature_log).anyhow("Failed to insert log")?;
            }
        }
    }

    Ok(count)
}

fn check_database_for_temp_logs(
    sensor: &Sensor,
    time: NaiveDateTime,
    service_provider: &ServiceProvider,
) -> Result<ListResult<TemperatureLog>> {
    let range_mins = sensor.sensor_row.log_interval.unwrap_or(300) as i64;
    let from = time - chrono::Duration::seconds(range_mins / 2);
    let to = time + chrono::Duration::seconds(range_mins / 2);

    let filter = TemperatureLogFilter {
        store_id: Some(EqualFilter::equal_to(&sensor.sensor_row.store_id)),
        datetime: Some(DatetimeFilter::date_range(from, to)),
        sensor: Some(SensorFilter {
            id: Some(EqualFilter::equal_to(&sensor.sensor_row.id)),
            ..Default::default()
        }),
        ..Default::default()
    };

    let connection = service_provider.connection()?;
    get_temperature_logs(&connection, None, Some(filter), None)
        .anyhow("Failed to get temperature logs")
}

#[derive(Debug, Deserialize)]
struct FileListing {
    entries: Vec<FileEntry>,
}

#[derive(Debug, Deserialize)]
struct FileEntry {
    name: String,
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

/// logger json file name format: <logger_id>\_<file_name>\_<relative_time>\_<absolute_time>.json
/// * <file_name> can contain underscores
/// * <relative_time> is an ISO 8601 duration. this is the time of the latest update to the file
/// * <absolute_time> is an ISO 8601 datetime. this is the time that the file was mounted by the logger
fn parse_logger_file_name(name: &str) -> Result<(String, String, Duration, NaiveDateTime)> {
    let (logger_id, rest_of_file_name) = name
        .trim_end_matches(".json")
        .split_once("_")
        .context("Invalid logger file name")?;

    let (rest_of_file_name, absolute_time) = rest_of_file_name
        .rsplit_once("_")
        .context("Invalid logger file name")?;

    let (file_name, relative_time) = rest_of_file_name
        .rsplit_once("_")
        .context("Invalid logger file name")?;

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
    fn anyhow(self, msg: &str) -> Result<A>;
}

impl<A, T: std::fmt::Debug> ToAnyhow<A> for Result<A, T> {
    fn anyhow(self, msg: &str) -> Result<A> {
        self.map_err(|e| anyhow::anyhow!("{}: {:?}", msg, e))
    }
}
