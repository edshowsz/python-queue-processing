-- Ensure database exists for the app (idempotent)
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = current_setting('POSTGRES_DB')
   ) THEN
      PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE ' || quote_ident(current_setting('POSTGRES_DB')));
   END IF;
END
$$;
