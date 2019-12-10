use futures::{try_ready, Future, Poll, Stream};
use tokio_sync::mpsc;

#[derive(Debug)]
pub struct Receiver(mpsc::Receiver<Never>);

#[derive(Debug, Clone)]
pub struct Handle(mpsc::Sender<Never>);

pub fn channel() -> (Handle, Receiver) {
    let (tx, rx) = mpsc::channel(1);
    (Handle(tx), Receiver(rx))
}

#[derive(Debug, Clone)]
enum Never {}

impl Future for Receiver {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.0.poll().map_err(|_| ())) {
            Some(never) => match never {},
            None => Ok(().into()),
        }
    }
}
