use std::sync::Arc;
use mongodb::ClientSession;
use teo_result::{Result, Error};

#[derive(Debug)]
pub struct OwnedSessionInner {
    session: * mut ClientSession,
}

impl OwnedSessionInner {
    pub fn new(session: ClientSession) -> Self {
        let boxed = Box::new(session);
        Self { session: Box::into_raw(boxed) }
    }

    pub fn session_mut(&self) -> &mut ClientSession {
        unsafe { &mut *self.session }
    }
}

impl Drop for OwnedSessionInner {
    fn drop(&mut self) {
        let _ = unsafe { Box::from_raw(self.session) };
    }
}

unsafe impl Send for OwnedSessionInner { }
unsafe impl Sync for OwnedSessionInner { }

#[derive(Clone, Debug)]
pub struct OwnedSession {
    inner: Arc<OwnedSessionInner>,
}

impl OwnedSession {

    pub fn new(client_session: ClientSession) -> Self {
        Self { inner: Arc::new(OwnedSessionInner::new(client_session)) }
    }

    pub fn client_session(&self) -> &mut ClientSession {
        self.inner.session_mut()
    }

    pub async fn start_transaction(&self) -> Result<()> {
        match self.inner.session_mut().start_transaction(None).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::new(e.to_string())),
        }
    }

    pub async fn commit_transaction(&self) -> Result<()> {
        match self.inner.session_mut().commit_transaction().await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::new(e.to_string())),
        }
    }

    pub async fn abort_transaction(&self) -> Result<()> {
        match self.inner.session_mut().abort_transaction().await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::new(e.to_string())),
        }
    }
}
