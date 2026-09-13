use chimaera::{
    AggregateSet, CancellationToken, Durability, GroupRequest, GroupScope, OpenOptions, Point,
    QueryOptions, SeriesKey, Storage,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Storage::open(directory.path(), OpenOptions::default())?;
    let series = [SeriesKey::new(1, 1, 1), SeriesKey::new(1, 1, 2)];
    storage.write_batch(
        vec![
            Point::new(series[0], 0, 2.0)?,
            Point::new(series[1], 0, 4.0)?,
        ],
        Durability::Sync,
    )?;
    let request = GroupRequest::fixed(series.to_vec(), 0, 10, 10, 0)?
        .with_scope(GroupScope::Combined)
        .with_aggregates(AggregateSet::ALL);
    let grouped = storage.group(request.clone())?;
    assert_eq!(grouped.rows.len(), 1);
    assert_eq!(grouped.rows[0].series, None);
    assert_eq!(grouped.rows[0].mean, Some(3.0));
    let cancellation = CancellationToken::new();
    let mut stream = storage.group_stream(request.with_options(QueryOptions {
        cancellation: Some(cancellation.clone()),
        ..QueryOptions::default()
    })?)?;
    cancellation.cancel();
    assert!(matches!(
        stream.next(),
        Some(Err(chimaera::Error::QueryCancelled))
    ));
    assert!(stream.next().is_none());
    drop(stream);
    storage.close()?;
    println!("query: Combined count=2, mean=3; cancellation observed");
    Ok(())
}
