use crate::error::Error;
use crate::executor::{Execute, Executor};
use crate::io::{PortalId, StatementId};
use crate::logger::QueryLogger;
use crate::message::{
    self, BackendMessageFormat, Bind, Close, CommandComplete, DataRow, ParameterDescription, Parse,
    ParseComplete, RowDescription,
};
use crate::statement::PgStatementMetadata;
use crate::{
    statement::PgStatement, PgArguments, PgConnection, PgQueryResult, PgRow, PgTypeInfo,
    PgValueFormat, Postgres,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_core::Stream;
use futures_util::TryStreamExt;
use sqlx_core::arguments::Arguments;
use sqlx_core::sql_str::SqlStr;
use sqlx_core::Either;
use std::{pin::pin, sync::Arc};

async fn prepare(
    conn: &mut PgConnection,
    sql: &str,
    arg_types: &[PgTypeInfo],
    metadata: Option<Arc<PgStatementMetadata>>,
    named: bool,
    resolve_column_origin: bool,
) -> Result<(StatementId, Arc<PgStatementMetadata>), Error> {
    let id = if named {
        let id = conn.inner.next_statement_id;
        conn.inner.next_statement_id = id.next();
        id
    } else {
        StatementId::UNNAMED
    };

    // build a list of type OIDs to send to the database in the PARSE command
    // we have not yet started the query sequence, so we are *safe* to cleanly make
    // additional queries here to get any missing OIDs
    let param_types = conn.resolve_types(arg_types).await?;

    // flush and wait until we are re-ready
    conn.wait_until_ready().await?;

    // next we send the PARSE command to the server
    conn.inner.stream.write_msg(Parse {
        param_types: &param_types,
        query: sql,
        statement: id,
    })?;

    if metadata.is_none() {
        // get the statement columns and parameters
        conn.inner
            .stream
            .write_msg(message::Describe::Statement(id))?;
    }

    // we ask for the server to immediately send us the result of the PARSE command
    conn.write_sync();
    conn.write_statements_to_close();
    conn.inner.stream.flush().await?;

    // indicates that the SQL query string is now successfully parsed and has semantic validity;
    // a proxy may leave it out and go on with the next message
    let next = conn.inner.stream.recv().await?;
    let next = if next.format == BackendMessageFormat::ParseComplete {
        next.decode::<ParseComplete>()?;
        None
    } else {
        Some(next)
    };

    let metadata = if let Some(metadata) = metadata {
        // each SYNC produces one READY FOR QUERY
        match next {
            Some(message) => conn.handle_ready_for_query(message)?,
            None => conn.recv_ready_for_query().await?,
        }

        // we already have metadata
        metadata
    } else {
        let parameters = match next {
            Some(message) => message.decode()?,
            None => recv_desc_params(conn).await?,
        };

        let row_desc = recv_desc_rows(conn).await?;

        // each SYNC produces one READY FOR QUERY
        conn.recv_ready_for_query().await?;

        let metadata = conn
            .resolve_statement_metadata::<true>(Some(parameters), row_desc, resolve_column_origin)
            .await?;

        // ensure that if we did fetch custom data, we wait until we are fully ready before
        // continuing
        conn.wait_until_ready().await?;

        metadata
    };

    Ok((id, metadata))
}

async fn recv_desc_params(conn: &mut PgConnection) -> Result<ParameterDescription, Error> {
    conn.inner.stream.recv_expect().await
}

async fn recv_desc_rows(conn: &mut PgConnection) -> Result<Option<RowDescription>, Error> {
    let rows: Option<RowDescription> = match conn.inner.stream.recv().await? {
        // describes the rows that will be returned when the statement is eventually executed
        message if message.format == BackendMessageFormat::RowDescription => {
            Some(message.decode()?)
        }

        // no data would be returned if this statement was executed
        message if message.format == BackendMessageFormat::NoData => None,

        message => {
            return Err(err_protocol!(
                "expecting RowDescription or NoData but received {:?}",
                message.format
            ));
        }
    };

    Ok(rows)
}

/// A statement parsed in the same round trip that executes it, until it is described.
struct Pipelined {
    /// The statement to cache once it is known to exist; never cached if unnamed.
    id: StatementId,
    /// `Describe` was sent and its reply has not been read yet.
    describing: bool,
    parameters: Option<ParameterDescription>,
}

impl PgConnection {
    // wait for CloseComplete to indicate a statement was closed
    pub(super) async fn wait_for_close_complete(&mut self, mut count: usize) -> Result<(), Error> {
        // we need to wait for the [CloseComplete] to be returned from the server
        while count > 0 {
            match self.inner.stream.recv().await? {
                message if message.format == BackendMessageFormat::PortalSuspended => {
                    // there was an open portal
                    // this can happen if the last time a statement was used it was not fully executed
                }

                message if message.format == BackendMessageFormat::CloseComplete => {
                    // successfully closed the statement (and freed up the server resources)
                    count -= 1;
                }

                message => {
                    return Err(err_protocol!(
                        "expecting PortalSuspended or CloseComplete but received {:?}",
                        message.format
                    ));
                }
            }
        }

        Ok(())
    }

    #[inline(always)]
    pub(crate) fn write_sync(&mut self) {
        self.inner
            .stream
            .write_msg(message::Sync)
            .expect("BUG: Sync should not be too big for protocol");

        // all SYNC messages will return a ReadyForQuery
        self.inner.pending_ready_for_query_count += 1;
    }

    /// A pipelined statement whose query was dropped before it was described may or may not
    /// exist on the server: close it with the next query (closing a missing one is no error).
    pub(super) fn queue_unconfirmed_close(&mut self) {
        if let Some(id) = self.inner.unconfirmed_statement.take() {
            self.inner.statements_to_close.push(id);
        }
    }

    /// Appends `Close` for the statements waiting for it, behind their own `Sync`, after a
    /// query already written: no round trip of their own, and their replies are drained by the
    /// next [`wait_until_ready`][Self::wait_until_ready]. Their own `Sync` keeps an error
    /// there out of the query's replies.
    fn write_statements_to_close(&mut self) {
        if self.inner.statements_to_close.is_empty() {
            return;
        }

        for id in std::mem::take(&mut self.inner.statements_to_close) {
            self.inner
                .stream
                .write_msg(Close::Statement(id))
                .expect("BUG: Close should not be too big for protocol");
        }

        self.write_sync();
    }

    /// Caches a pipelined statement once the server has it.
    fn confirm_statement(
        &mut self,
        sql: &str,
        id: StatementId,
        metadata: &Arc<PgStatementMetadata>,
    ) {
        if self.inner.unconfirmed_statement != Some(id) {
            // unnamed, or already cached
            return;
        }

        self.inner.unconfirmed_statement = None;

        if let Some((evicted, _)) = self
            .inner
            .cache_statement
            .insert(sql, (id, Arc::clone(metadata)))
        {
            self.inner.statements_to_close.push(evicted);
        }
    }

    /// Writes the messages of one round trip followed by `Sync`; if any fails to encode,
    /// none of them is sent.
    fn write_pipeline(
        &mut self,
        write: impl FnOnce(&mut Self) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let unflushed = self.inner.stream.write_buffer().get().len();

        if let Err(error) = write(self) {
            self.inner
                .stream
                .write_buffer_mut()
                .truncate_unflushed(unflushed);

            return Err(error);
        }

        self.write_sync();

        Ok(())
    }

    fn write_bind_execute(
        &mut self,
        statement: StatementId,
        arguments: &PgArguments,
        num_params: u16,
    ) -> Result<(), Error> {
        // bind to attach the arguments to the statement and create a portal
        self.inner.stream.write_msg(Bind {
            portal: PortalId::UNNAMED,
            statement,
            formats: &[PgValueFormat::Binary],
            num_params,
            params: &arguments.buffer,
            result_formats: &[PgValueFormat::Binary],
        })?;

        // executes the portal up to the passed limit
        // the protocol-level limit acts nearly identically to the `LIMIT` in SQL
        self.inner.stream.write_msg(message::Execute {
            portal: PortalId::UNNAMED,
            // Non-zero limits cause query plan pessimization by disabling parallel workers:
            // https://github.com/launchbadge/sqlx/issues/3673
            limit: 0,
        })?;
        // From https://www.postgresql.org/docs/current/protocol-flow.html:
        //
        // "An unnamed portal is destroyed at the end of the transaction, or as
        // soon as the next Bind statement specifying the unnamed portal as
        // destination is issued. (Note that a simple Query message also
        // destroys the unnamed portal."

        // we ask the database server to close the unnamed portal and free the associated resources
        // earlier - after the execution of the current query.
        if self.inner.close_unnamed_portal {
            self.inner
                .stream
                .write_msg(Close::Portal(PortalId::UNNAMED))?;
        }

        Ok(())
    }

    async fn get_or_prepare(
        &mut self,
        sql: &str,
        parameters: &[PgTypeInfo],
        persistent: bool,
        // optional metadata that was provided by the user, this means they are reusing
        // a statement object
        metadata: Option<Arc<PgStatementMetadata>>,
        resolve_column_origin: bool,
        // name the statement even if it will not be cached: `describe` needs a name for
        // `EXPLAIN EXECUTE`, and closes it
        force_named: bool,
    ) -> Result<(StatementId, Arc<PgStatementMetadata>), Error> {
        if let Some(statement) = self.inner.cache_statement.get_mut(sql) {
            return Ok((*statement).clone());
        }

        self.queue_unconfirmed_close();

        // a named statement that is not cached would never be closed
        let cache = persistent && self.inner.cache_statement.is_enabled();

        let statement = prepare(
            self,
            sql,
            parameters,
            metadata,
            cache || force_named,
            resolve_column_origin,
        )
        .await?;

        if cache {
            if let Some((id, _)) = self.inner.cache_statement.insert(sql, statement.clone()) {
                self.inner.statements_to_close.push(id);
            }
        }

        Ok(statement)
    }

    pub(crate) async fn run<'e, 'c: 'e, 'q: 'e>(
        &'c mut self,
        query: SqlStr,
        arguments: Option<PgArguments>,
        persistent: bool,
        metadata_opt: Option<Arc<PgStatementMetadata>>,
    ) -> Result<impl Stream<Item = Result<Either<PgQueryResult, PgRow>, Error>> + 'e, Error> {
        let mut logger = QueryLogger::new(query, self.inner.log_settings.clone());
        let sql = logger.sql().as_str();

        // before we continue, wait until we are "ready" to accept more queries
        self.wait_until_ready().await?;

        self.queue_unconfirmed_close();

        let mut metadata: Arc<PgStatementMetadata>;
        let mut pipelined = None;

        let format = if let Some(mut arguments) = arguments {
            // Check this before we write anything to the stream.
            //
            // Note: Postgres actually interprets this value as unsigned,
            // making the max number of parameters 65535, not 32767
            // https://github.com/launchbadge/sqlx/issues/3464
            // https://www.postgresql.org/docs/current/limits.html
            let num_params = u16::try_from(arguments.len()).map_err(|_| {
                err_protocol!(
                    "PgConnection::run(): too many arguments for query: {}",
                    arguments.len()
                )
            })?;

            if let Some((statement, cached)) = self.inner.cache_statement.get_mut(sql).cloned() {
                metadata = cached;

                // patch holes created during encoding
                arguments.apply_patches(self, &metadata.parameters).await?;

                // consume messages till `ReadyForQuery` before bind and execute
                self.wait_until_ready().await?;

                self.write_pipeline(|conn| {
                    conn.write_bind_execute(statement, &arguments, num_params)
                })?;
            } else {
                // Parse, Describe, Bind, Execute and Sync go out together: one round trip, and
                // nothing in between for a transaction-pooling proxy to split. So whatever may
                // query the server (resolving type names into OIDs) happens first.
                let param_types = self.resolve_types(&arguments.types).await?;
                let holes = arguments.resolve_holes(self).await?;

                self.wait_until_ready().await?;

                let id = if persistent && self.inner.cache_statement.is_enabled() {
                    let id = self.inner.next_statement_id;
                    self.inner.next_statement_id = id.next();
                    id
                } else {
                    StatementId::UNNAMED
                };

                let describe = metadata_opt.is_none();

                // Without a statement object, the parameters are the ones declared in `Parse`:
                // the server describes them the same.
                metadata = metadata_opt.unwrap_or_else(|| {
                    Arc::new(PgStatementMetadata {
                        parameters: arguments.types.clone(),
                        ..Default::default()
                    })
                });

                arguments.apply_resolved_patches(&metadata.parameters, &holes);

                self.write_pipeline(|conn| {
                    conn.inner.stream.write_msg(Parse {
                        param_types: &param_types,
                        query: sql,
                        statement: id,
                    })?;

                    if describe {
                        conn.inner
                            .stream
                            .write_msg(message::Describe::Statement(id))?;
                    }

                    conn.write_bind_execute(id, &arguments, num_params)
                })?;

                if id != StatementId::UNNAMED {
                    self.inner.unconfirmed_statement = Some(id);
                }

                pipelined = Some(Pipelined {
                    id,
                    describing: describe,
                    parameters: None,
                });
            }

            // prepared statements are binary
            PgValueFormat::Binary
        } else {
            // Query will trigger a ReadyForQuery
            self.queue_simple_query(sql)?;

            // metadata starts out as "nothing"
            metadata = Arc::new(PgStatementMetadata::default());

            // and unprepared statements are text
            PgValueFormat::Text
        };

        self.write_statements_to_close();

        self.inner.stream.flush().await?;

        Ok(try_stream! {
            loop {
                let message = self.inner.stream.recv().await?;

                match message.format {
                    BackendMessageFormat::ParseComplete | BackendMessageFormat::BindComplete => {
                        // a statement that was not described exists once parsed
                        if let Some(Pipelined { id, describing: false, .. }) = pipelined {
                            self.confirm_statement(logger.sql().as_str(), id, &metadata);
                        }
                    }

                    BackendMessageFormat::ParameterDescription
                        if pipelined.as_ref().is_some_and(|p| p.describing) =>
                    {
                        if let Some(p) = &mut pipelined {
                            p.parameters = Some(message.decode()?);
                        }
                    }

                    BackendMessageFormat::RowDescription | BackendMessageFormat::NoData
                        if pipelined.as_ref().is_some_and(|p| p.describing) =>
                    {
                        let Some(p) = &mut pipelined else { unreachable!() };
                        p.describing = false;
                        let id = p.id;
                        let parameters = p.parameters.take();

                        let rows: Option<RowDescription> =
                            if message.format == BackendMessageFormat::RowDescription {
                                Some(message.decode()?)
                            } else {
                                None
                            };

                        if !self.has_unknown_types(parameters.as_ref(), rows.as_ref()) {
                            metadata = self
                                .resolve_statement_metadata::<false>(parameters, rows, false)
                                .await?;

                            self.confirm_statement(logger.sql().as_str(), id, &metadata);
                            continue;
                        }

                        // A type this connection has not resolved yet: looking it up is a
                        // query, which has to wait for this one to finish. Its rows are held
                        // until then, so they decode with the full types as usual.
                        let mut held = Vec::new();

                        loop {
                            let message = self.inner.stream.recv().await?;

                            match message.format {
                                BackendMessageFormat::DataRow => {
                                    held.push(Either::Right(message.decode::<DataRow>()?));
                                }

                                BackendMessageFormat::CommandComplete => {
                                    let cc: CommandComplete = message.decode()?;
                                    held.push(Either::Left(cc.rows_affected()));
                                }

                                BackendMessageFormat::ReadyForQuery => {
                                    self.handle_ready_for_query(message)?;
                                    break;
                                }

                                BackendMessageFormat::BindComplete
                                | BackendMessageFormat::CloseComplete
                                | BackendMessageFormat::EmptyQueryResponse
                                | BackendMessageFormat::PortalSuspended => {}

                                _ => {
                                    return Err(err_protocol!(
                                        "execute: unexpected message: {:?}",
                                        message.format
                                    ));
                                }
                            }
                        }

                        // the lookup is a query of its own: keep it from closing this statement
                        let unconfirmed = self.inner.unconfirmed_statement.take();
                        let resolved = self
                            .resolve_statement_metadata::<true>(parameters, rows, false)
                            .await;
                        self.inner.unconfirmed_statement = unconfirmed;

                        metadata = resolved?;
                        self.confirm_statement(logger.sql().as_str(), id, &metadata);

                        for item in held {
                            match item {
                                Either::Left(rows_affected) => {
                                    logger.increase_rows_affected(rows_affected);
                                    r#yield!(Either::Left(PgQueryResult { rows_affected }));
                                }

                                Either::Right(data) => {
                                    logger.increment_rows_returned();
                                    r#yield!(Either::Right(PgRow {
                                        data,
                                        format,
                                        metadata: Arc::clone(&metadata),
                                    }));
                                }
                            }
                        }

                        // this query's ReadyForQuery was read above
                        break;
                    }

                    BackendMessageFormat::ParameterDescription
                    | BackendMessageFormat::NoData
                    // unnamed portal has been closed
                    | BackendMessageFormat::CloseComplete
                    => {
                        // harmless messages to ignore
                    }

                    // "Execute phase is always terminated by the appearance of
                    // exactly one of these messages: CommandComplete,
                    // EmptyQueryResponse (if the portal was created from an
                    // empty query string), ErrorResponse, or PortalSuspended"
                    BackendMessageFormat::CommandComplete => {
                        // a SQL command completed normally
                        let cc: CommandComplete = message.decode()?;

                        let rows_affected = cc.rows_affected();
                        logger.increase_rows_affected(rows_affected);
                        r#yield!(Either::Left(PgQueryResult {
                            rows_affected,
                        }));
                    }

                    BackendMessageFormat::EmptyQueryResponse => {
                        // empty query string passed to an unprepared execute
                    }

                    // Message::ErrorResponse is handled in self.stream.recv()

                    // incomplete query execution has finished
                    BackendMessageFormat::PortalSuspended => {}

                    // indicates that a *new* set of rows are about to be returned
                    BackendMessageFormat::RowDescription => {
                        let new_metadata = self.resolve_statement_metadata::<false>(
                            None,
                            Some(message.decode()?),
                            false,
                        ).await?;

                        metadata = new_metadata;
                    }

                    BackendMessageFormat::DataRow => {
                        logger.increment_rows_returned();

                        // one of the set of rows returned by a SELECT, FETCH, etc query
                        let data: DataRow = message.decode()?;
                        let row = PgRow {
                            data,
                            format,
                            metadata: Arc::clone(&metadata),
                        };

                        r#yield!(Either::Right(row));
                    }

                    BackendMessageFormat::ReadyForQuery => {
                        // processing of the query string is complete
                        self.handle_ready_for_query(message)?;
                        break;
                    }

                    _ => {
                        return Err(err_protocol!(
                            "execute: unexpected message: {:?}",
                            message.format
                        ));
                    }
                }
            }

            Ok(())
        })
    }
}

impl<'c> Executor<'c> for &'c mut PgConnection {
    type Database = Postgres;

    fn fetch_many<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        // False positive: https://github.com/rust-lang/rust-clippy/issues/12560
        #[allow(clippy::map_clone)]
        let metadata = query.statement().map(|s| Arc::clone(&s.metadata));
        let arguments = query.take_arguments().map_err(Error::Encode);
        let persistent = query.persistent();
        let sql = query.sql();

        Box::pin(try_stream! {
            let arguments = arguments?;
            let mut s = pin!(self.run(sql, arguments, persistent, metadata).await?);

            while let Some(v) = s.try_next().await? {
                r#yield!(v);
            }

            Ok(())
        })
    }

    fn fetch_optional<'e, 'q, E>(self, mut query: E) -> BoxFuture<'e, Result<Option<PgRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        // False positive: https://github.com/rust-lang/rust-clippy/issues/12560
        #[allow(clippy::map_clone)]
        let metadata = query.statement().map(|s| Arc::clone(&s.metadata));
        let arguments = query.take_arguments().map_err(Error::Encode);
        let persistent = query.persistent();

        Box::pin(async move {
            let sql = query.sql();
            let arguments = arguments?;
            let mut s = pin!(self.run(sql, arguments, persistent, metadata).await?);

            // With deferred constraints we need to check all responses as we
            // could get a OK response (with uncommitted data), only to get an
            // error response after (when the deferred constraint is actually
            // checked).
            let mut ret = None;
            while let Some(result) = s.try_next().await? {
                match result {
                    Either::Right(r) if ret.is_none() => ret = Some(r),
                    _ => {}
                }
            }
            Ok(ret)
        })
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<PgStatement, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            self.wait_until_ready().await?;

            let (_, metadata) = self
                .get_or_prepare(sql.as_str(), parameters, true, None, true, false)
                .await?;

            Ok(PgStatement { sql, metadata })
        })
    }

    #[cfg(feature = "offline")]
    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'e, Result<crate::describe::Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            self.wait_until_ready().await?;

            let (stmt_id, metadata) = self
                .get_or_prepare(sql.as_str(), &[], true, None, true, true)
                .await?;

            let nullable = self.get_nullable_for_columns(stmt_id, &metadata).await;

            // named only for `EXPLAIN EXECUTE`: nothing else would close it
            if !self.inner.cache_statement.contains_key(sql.as_str()) {
                self.inner.statements_to_close.push(stmt_id);
            }

            let nullable = nullable?;

            Ok(crate::describe::Describe {
                columns: metadata.columns.clone(),
                nullable,
                parameters: Some(Either::Left(metadata.parameters.clone())),
            })
        })
    }
}
