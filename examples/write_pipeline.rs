use chimaera::{Durability, OpenOptions, Point, SeriesKey, Storage, WriteStatus, WriteTicket};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

fn observe(mut ticket: WriteTicket) -> chimaera::Result<()> {
    loop {
        match ticket.wait_until(Instant::now() + Duration::from_secs(1)) {
            WriteStatus::Pending => continue, // Keep this ticket; do not enqueue the batch again.
            WriteStatus::Durable(receipt) => {
                assert!(receipt.durable);
                return Ok(());
            }
            WriteStatus::Visible(_) => {
                return Err(chimaera::Error::Invalid("expected durable receipt".into()));
            }
            WriteStatus::Failed(_) | WriteStatus::OutcomeUnknown(_) => {
                return ticket.wait().map(|_| ());
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Storage::open(directory.path(), OpenOptions::default())?;
    let series = SeriesKey::new(1, 1, 42);
    let mut pending = VecDeque::new();
    let mut first_error = None;
    for timestamp in 0..16 {
        if pending.len() == 4
            && let Some(ticket) = pending.pop_front()
            && let Err(error) = observe(ticket)
        {
            first_error = Some(error);
            break;
        }
        match storage.enqueue_batch(
            vec![Point::new(series, timestamp, 1.0)?],
            Durability::GroupCommit,
        ) {
            Ok(ticket) => pending.push_back(ticket),
            Err(error) => {
                first_error = Some(error);
                break;
            }
        }
    }
    // Observe every accepted batch, including those submitted before a later error.
    for ticket in pending {
        if let Err(error) = observe(ticket) {
            first_error.get_or_insert(error);
        }
    }
    let close_result = storage.close();
    if let Some(error) = first_error {
        return Err(error.into());
    }
    close_result?;
    println!("pipeline: 16 durable batches; at most 4 tickets held");
    Ok(())
}
